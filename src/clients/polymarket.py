"""Polymarket CLOB API client wrapper."""

import json
from typing import Optional
from datetime import datetime, timezone
from dataclasses import dataclass
import httpx
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import BookParams

from src.config import config


@dataclass
class MarketPrices:
    """Current prices for a binary market."""

    market_id: str
    yes_token_id: str
    no_token_id: str
    yes_ask: Optional[float]
    yes_bid: Optional[float]
    no_ask: Optional[float]
    no_bid: Optional[float]
    yes_liquidity: float
    no_liquidity: float
    timestamp: datetime

    # Minimum tick on Polymarket is $0.02 - prices at this level with no liquidity are fake
    MIN_TICK = 0.02
    # Minimum liquidity to consider a price valid (in shares/USD)
    MIN_LIQUIDITY = 10.0

    @property
    def has_valid_prices(self) -> bool:
        """Check if prices are real (not just minimum tick with no liquidity)."""
        # If both prices are at minimum tick, check liquidity
        if self.yes_ask == self.MIN_TICK and self.no_ask == self.MIN_TICK:
            # Need actual liquidity on both sides
            return self.yes_liquidity >= self.MIN_LIQUIDITY and self.no_liquidity >= self.MIN_LIQUIDITY
        # If either price is above minimum tick, it's likely real
        return self.yes_ask is not None and self.no_ask is not None

    @property
    def combined_ask(self) -> Optional[float]:
        """Combined cost to buy both sides."""
        if self.yes_ask and self.no_ask:
            return self.yes_ask + self.no_ask
        return None

    @property
    def arbitrage_profit_pct(self) -> Optional[float]:
        """Potential arbitrage profit percentage."""
        if self.combined_ask and self.combined_ask < 1.0:
            profit = 1.0 - self.combined_ask
            return profit / self.combined_ask
        return None

    @property
    def has_arbitrage(self) -> bool:
        """Check if REAL arbitrage opportunity exists (not fake min-tick prices)."""
        if not self.has_valid_prices:
            return False
        if self.combined_ask is None or self.combined_ask >= 1.0:
            return False
        # Must have minimum liquidity on both sides to actually trade
        if self.yes_liquidity < self.MIN_LIQUIDITY or self.no_liquidity < self.MIN_LIQUIDITY:
            return False
        return True


@dataclass
class Market:
    """Polymarket market data."""

    id: str
    condition_id: str
    question: str
    slug: str
    asset: str  # BTC, ETH, XRP, SOL
    yes_token_id: str
    no_token_id: str
    end_date: Optional[datetime]
    active: bool


class PolymarketClient:
    """
    Wrapper around py-clob-client with convenience methods
    for Polymarket binary markets and arbitrage detection.
    """

    # Market slug patterns for 15-minute crypto markets (when active)
    CRYPTO_PATTERNS = ["btc-updown-15m", "eth-updown-15m", "xrp-updown-15m", "sol-updown-15m"]

    def __init__(self, mode: str = "read"):
        """
        Initialize client.

        Args:
            mode: "read" for read-only, "trade" for full trading access
        """
        self.mode = mode
        self.host = config.clob_host
        self.gamma_host = config.gamma_host

        if mode == "trade" and config.has_wallet:
            self.client = ClobClient(
                self.host,
                key=config.polygon_private_key,
                chain_id=config.chain_id,
                signature_type=1,  # For email/Magic wallet style
                funder=config.polygon_wallet_address,
            )
            self.client.set_api_creds(self.client.create_or_derive_api_creds())
        else:
            # Read-only mode
            self.client = ClobClient(self.host)

        # HTTP client for direct API calls
        self.http = httpx.Client(timeout=30)

    def get_active_15min_markets(self) -> list[Market]:
        """
        Fetch currently active crypto up/down markets (5m, 15m, 4h).
        Uses the events endpoint ordered by newest first.

        Returns list of active markets for BTC, ETH, XRP, SOL.
        """
        markets = []

        try:
            # Query for most recent events, not closed
            # order=id with ascending=false gets newest markets first
            response = self.http.get(
                f"{self.gamma_host}/events",
                params={
                    "limit": 100,
                    "order": "id",
                    "ascending": "false",
                    "closed": "false",
                },
            )
            response.raise_for_status()
            events = response.json()

            for event in events:
                title = (event.get("title", "") or "").lower()
                slug = (event.get("slug", "") or "").lower()

                # Check if this is a crypto up/down event (5m or 15m only, not 4h)
                is_updown = (
                    "updown" in slug
                    and any(asset in slug for asset in ["btc", "eth", "xrp", "sol"])
                    and ("5m" in slug or "15m" in slug)
                    and "4h" not in slug
                )

                if is_updown:
                    # Get the market from this event
                    for market_data in event.get("markets", []):
                        # Skip closed markets
                        if market_data.get("closed"):
                            continue

                        # Skip markets not accepting orders (not in active trading window)
                        if not market_data.get("acceptingOrders", False):
                            continue

                        # Parse clobTokenIds - comes as JSON string from API
                        clob_token_ids_raw = market_data.get("clobTokenIds")
                        if isinstance(clob_token_ids_raw, str):
                            try:
                                clob_token_ids = json.loads(clob_token_ids_raw)
                            except json.JSONDecodeError:
                                continue
                        else:
                            clob_token_ids = clob_token_ids_raw or []

                        # Also check tokens array as fallback
                        tokens = market_data.get("tokens", [])

                        # Get YES and NO token IDs
                        if len(clob_token_ids) >= 2:
                            yes_token_id = clob_token_ids[0]
                            no_token_id = clob_token_ids[1]
                        elif len(tokens) >= 2:
                            yes_token_id = tokens[0].get("token_id")
                            no_token_id = tokens[1].get("token_id")
                        else:
                            continue

                        if not yes_token_id or not no_token_id:
                            continue

                        asset = self._extract_asset(slug or title)
                        markets.append(
                            Market(
                                id=market_data.get("id"),
                                condition_id=market_data.get("conditionId"),
                                question=market_data.get("question", event.get("title", "")),
                                slug=slug,
                                asset=asset,
                                yes_token_id=yes_token_id,
                                no_token_id=no_token_id,
                                end_date=self._parse_date(market_data.get("endDate")),
                                active=market_data.get("active", True),
                            )
                        )

        except Exception as e:
            print(f"Error fetching events from Gamma API: {e}")

        return markets

    def _extract_asset(self, slug: str) -> str:
        """Extract asset type from market slug."""
        slug_upper = slug.upper()
        for asset in ["BTC", "ETH", "XRP", "SOL"]:
            if asset in slug_upper:
                return asset
        return "UNKNOWN"

    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse ISO date string."""
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except:
            return None

    def get_token_price(self, token_id: str, side: str = "buy") -> Optional[float]:
        """
        Get the current price for a token using CLOB /price endpoint.

        Args:
            token_id: The token ID
            side: "buy" or "sell"

        Returns the price or None if unavailable.
        """
        try:
            response = self.http.get(
                f"{self.host}/price",
                params={"token_id": token_id, "side": side}
            )
            if response.status_code == 200:
                return float(response.json().get("price", 0))
        except Exception as e:
            print(f"Error fetching price for {token_id}: {e}")
        return None

    def get_orderbook(self, token_id: str) -> dict:
        """
        Get current order book for a token.

        Returns dict with bids, asks, best_bid, best_ask.
        """
        try:
            book = self.client.get_order_book(token_id)

            bids = [(float(b.price), float(b.size)) for b in book.bids] if book.bids else []
            asks = [(float(a.price), float(a.size)) for a in book.asks] if book.asks else []

            return {
                "token_id": token_id,
                "bids": bids,
                "asks": asks,
                "best_bid": bids[0][0] if bids else None,
                "best_ask": asks[0][0] if asks else None,
                "bid_liquidity": sum(size for _, size in bids[:5]),
                "ask_liquidity": sum(size for _, size in asks[:5]),
            }
        except Exception:
            # Silently return empty book - many new markets don't have order books yet
            return {
                "token_id": token_id,
                "bids": [],
                "asks": [],
                "best_bid": None,
                "best_ask": None,
                "bid_liquidity": 0,
                "ask_liquidity": 0,
            }

    def get_market_prices(self, market: Market) -> MarketPrices:
        """
        Get current prices for a market using the /price endpoint.

        Returns MarketPrices with yes/no prices and liquidity.
        """
        # Use /price endpoint for accurate buy/sell prices
        yes_buy = self.get_token_price(market.yes_token_id, "buy")
        yes_sell = self.get_token_price(market.yes_token_id, "sell")
        no_buy = self.get_token_price(market.no_token_id, "buy")
        no_sell = self.get_token_price(market.no_token_id, "sell")

        # Also get order book for liquidity info
        yes_book = self.get_orderbook(market.yes_token_id)
        no_book = self.get_orderbook(market.no_token_id)

        return MarketPrices(
            market_id=market.condition_id,
            yes_token_id=market.yes_token_id,
            no_token_id=market.no_token_id,
            yes_ask=yes_buy,  # What you pay to buy YES (UP)
            yes_bid=yes_sell,  # What you get to sell YES (UP)
            no_ask=no_buy,  # What you pay to buy NO (DOWN)
            no_bid=no_sell,  # What you get to sell NO (DOWN)
            yes_liquidity=yes_book["ask_liquidity"],
            no_liquidity=no_book["ask_liquidity"],
            timestamp=datetime.now(timezone.utc),
        )

    def get_all_market_prices(self) -> list[tuple[Market, MarketPrices]]:
        """
        Get prices for all active 15-minute crypto markets.

        Returns list of (market, prices) tuples.
        """
        markets = self.get_active_15min_markets()
        results = []

        for market in markets:
            prices = self.get_market_prices(market)
            results.append((market, prices))

        return results

    def find_arbitrage_opportunities(
        self, min_profit_pct: float = 0.025, min_liquidity: float = 50.0
    ) -> list[tuple[Market, MarketPrices]]:
        """
        Find markets with arbitrage opportunities.

        Args:
            min_profit_pct: Minimum profit percentage (0.025 = 2.5%)
            min_liquidity: Minimum liquidity on each side in USD

        Returns list of (market, prices) with arbitrage opportunities.
        """
        opportunities = []

        for market, prices in self.get_all_market_prices():
            if not prices.has_arbitrage:
                continue

            profit_pct = prices.arbitrage_profit_pct
            if profit_pct < min_profit_pct:
                continue

            if prices.yes_liquidity < min_liquidity or prices.no_liquidity < min_liquidity:
                continue

            opportunities.append((market, prices))

        # Sort by profit percentage descending
        opportunities.sort(key=lambda x: x[1].arbitrage_profit_pct or 0, reverse=True)

        return opportunities

    def close(self):
        """Close HTTP client."""
        self.http.close()
