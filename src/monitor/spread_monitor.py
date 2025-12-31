"""
Spread Monitor for Polymarket Delta-Neutral Strategy.

Monitors real-time spreads on 5m/15m crypto markets and detects
when combined UP + DOWN < $1.00 (profit opportunity).
"""

import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Optional
from dataclasses import dataclass, field
import httpx

from src.config import config
from src.models.database import (
    init_db, SpreadSnapshot, Opportunity, MarketWindow,
    DailyStats, MonitorStatus
)
from .websocket_client import PolymarketWebSocket, OrderBookUpdate


@dataclass
class MarketState:
    """Current state of a single market (UP/DOWN pair)."""
    asset: str
    market_id: str
    timeframe: str  # "5m" or "15m"

    # Token IDs
    up_token: str
    down_token: str

    # Current prices (best asks)
    up_ask: Optional[float] = None
    down_ask: Optional[float] = None

    # Current liquidity
    up_liquidity: float = 0.0
    down_liquidity: float = 0.0

    # Timestamps
    up_updated: Optional[datetime] = None
    down_updated: Optional[datetime] = None

    # Opportunity tracking
    opportunity_start: Optional[datetime] = None
    opportunity_best_spread: float = 0.0

    @property
    def combined(self) -> Optional[float]:
        """Combined cost to buy both sides."""
        if self.up_ask and self.down_ask:
            return self.up_ask + self.down_ask
        return None

    @property
    def spread(self) -> Optional[float]:
        """Spread (profit potential). Positive = opportunity."""
        if self.combined:
            return 1.0 - self.combined
        return None

    @property
    def spread_pct(self) -> Optional[float]:
        """Spread as percentage of cost."""
        if self.combined and self.combined > 0:
            return ((1.0 - self.combined) / self.combined) * 100
        return None

    @property
    def has_opportunity(self) -> bool:
        """Check if there's a profit opportunity."""
        return self.combined is not None and self.combined < 1.0

    @property
    def has_valid_prices(self) -> bool:
        """Check if we have valid prices on both sides."""
        if self.up_ask is None or self.down_ask is None:
            return False
        # Filter out min-tick placeholder prices
        if self.up_ask <= 0.02 or self.down_ask <= 0.02:
            return False
        # Filter out obviously fake combined costs
        if self.combined and self.combined < 0.50:
            return False
        return True

    @property
    def max_position(self) -> float:
        """Maximum position size based on available liquidity."""
        return min(self.up_liquidity, self.down_liquidity)


class SpreadMonitor:
    """
    Main monitoring system for Polymarket spreads.

    - Discovers active 5m/15m crypto markets
    - Subscribes to real-time price updates via WebSocket
    - Detects and records opportunities (combined < $1.00)
    - Stores snapshots for historical analysis
    """

    # How often to take snapshots (seconds)
    SNAPSHOT_INTERVAL = 30

    # How often to refresh market list (seconds)
    MARKET_REFRESH_INTERVAL = 60

    # Minimum spread to consider an opportunity (avoid noise)
    MIN_SPREAD_PCT = 0.5  # 0.5%

    def __init__(self, database_url: str):
        """Initialize the spread monitor."""
        self.database_url = database_url
        self._engine, self._Session = init_db(database_url)

        # Market state tracking
        self._markets: dict[str, MarketState] = {}  # market_id -> state
        self._token_to_market: dict[str, str] = {}  # token_id -> market_id

        # WebSocket client
        self._ws_client: Optional[PolymarketWebSocket] = None

        # HTTP client for market discovery
        self._http = httpx.AsyncClient(timeout=30)

        # State
        self._running = False
        self._connected = False
        self._last_snapshot = datetime.now(timezone.utc)
        self._last_market_refresh = datetime.min.replace(tzinfo=timezone.utc)

        # Stats
        self._snapshots_recorded = 0
        self._opportunities_detected = 0

    async def start(self):
        """Start the spread monitor."""
        print("[Monitor] Starting spread monitor...")
        self._running = True

        # Create WebSocket client
        self._ws_client = PolymarketWebSocket(
            on_book_update=self._on_book_update,
            on_connect=self._on_connect,
            on_disconnect=self._on_disconnect,
        )

        # Start tasks
        await asyncio.gather(
            self._ws_client.run(),
            self._market_refresh_loop(),
            self._snapshot_loop(),
            return_exceptions=True,
        )

    async def stop(self):
        """Stop the spread monitor."""
        print("[Monitor] Stopping...")
        self._running = False

        if self._ws_client:
            await self._ws_client.stop()

        await self._http.aclose()
        print("[Monitor] Stopped")

    def _on_connect(self):
        """Called when WebSocket connects."""
        self._connected = True
        print("[Monitor] WebSocket connected")

    def _on_disconnect(self):
        """Called when WebSocket disconnects."""
        self._connected = False
        print("[Monitor] WebSocket disconnected")

    def _on_book_update(self, token_id: str, update: OrderBookUpdate):
        """
        Handle order book update from WebSocket.
        This is called for every price change.
        """
        market_id = self._token_to_market.get(token_id)
        if not market_id:
            return

        market = self._markets.get(market_id)
        if not market:
            return

        now = datetime.now(timezone.utc)

        # Update the appropriate side
        if token_id == market.up_token:
            market.up_ask = update.best_ask
            market.up_liquidity = update.ask_liquidity
            market.up_updated = now
        elif token_id == market.down_token:
            market.down_ask = update.best_ask
            market.down_liquidity = update.ask_liquidity
            market.down_updated = now

        # Check for opportunity
        self._check_opportunity(market)

    def _check_opportunity(self, market: MarketState):
        """Check if market has an opportunity and handle it."""
        if not market.has_valid_prices:
            # If we were tracking an opportunity, close it
            if market.opportunity_start:
                self._close_opportunity(market)
            return

        if market.has_opportunity:
            spread_pct = market.spread_pct or 0

            # Filter out tiny spreads
            if spread_pct < self.MIN_SPREAD_PCT:
                if market.opportunity_start:
                    self._close_opportunity(market)
                return

            if market.opportunity_start is None:
                # New opportunity detected!
                self._open_opportunity(market)
            else:
                # Update best spread
                if market.spread and market.spread > market.opportunity_best_spread:
                    market.opportunity_best_spread = market.spread

        else:
            # No opportunity - close if we had one
            if market.opportunity_start:
                self._close_opportunity(market)

    def _open_opportunity(self, market: MarketState):
        """Record a new opportunity opening."""
        now = datetime.now(timezone.utc)
        market.opportunity_start = now
        market.opportunity_best_spread = market.spread or 0

        self._opportunities_detected += 1

        # Log to database
        session = self._Session()
        try:
            opp = Opportunity(
                detected_at=now,
                asset=market.asset,
                market_id=market.market_id,
                up_ask=market.up_ask,
                down_ask=market.down_ask,
                combined=market.combined,
                spread=market.spread or 0,
                spread_pct=market.spread_pct or 0,
                up_liquidity=market.up_liquidity,
                down_liquidity=market.down_liquidity,
                max_position=market.max_position,
            )
            session.add(opp)
            session.commit()

            print(f"[OPPORTUNITY] {market.asset} | "
                  f"UP=${market.up_ask:.3f} + DOWN=${market.down_ask:.3f} = "
                  f"${market.combined:.3f} | "
                  f"Spread: {market.spread_pct:.2f}% | "
                  f"Liq: {market.max_position:.0f}")

        except Exception as e:
            print(f"[Monitor] DB error logging opportunity: {e}")
            session.rollback()
        finally:
            session.close()

    def _close_opportunity(self, market: MarketState):
        """Record an opportunity closing."""
        if not market.opportunity_start:
            return

        now = datetime.now(timezone.utc)
        duration = (now - market.opportunity_start).total_seconds()

        # Update the opportunity record
        session = self._Session()
        try:
            # Find the most recent opportunity for this market
            opp = session.query(Opportunity).filter(
                Opportunity.market_id == market.market_id,
                Opportunity.resolved_at.is_(None)
            ).order_by(Opportunity.detected_at.desc()).first()

            if opp:
                opp.resolved_at = now
                opp.duration_seconds = duration
                opp.best_spread = market.opportunity_best_spread
                opp.best_spread_pct = (market.opportunity_best_spread / (1 - market.opportunity_best_spread)) * 100 if market.opportunity_best_spread < 1 else 0
                session.commit()

                print(f"[OPP CLOSED] {market.asset} | "
                      f"Duration: {duration:.1f}s | "
                      f"Best spread: {opp.best_spread_pct:.2f}%")

        except Exception as e:
            print(f"[Monitor] DB error closing opportunity: {e}")
            session.rollback()
        finally:
            session.close()

        # Reset tracking
        market.opportunity_start = None
        market.opportunity_best_spread = 0

    async def _market_refresh_loop(self):
        """Periodically refresh the list of active markets."""
        while self._running:
            try:
                now = datetime.now(timezone.utc)
                if (now - self._last_market_refresh).total_seconds() >= self.MARKET_REFRESH_INTERVAL:
                    await self._refresh_markets()
                    self._last_market_refresh = now
            except Exception as e:
                print(f"[Monitor] Market refresh error: {e}")

            await asyncio.sleep(5)

    async def _refresh_markets(self):
        """Fetch active markets and subscribe to their tokens."""
        print("[Monitor] Refreshing markets...")

        try:
            response = await self._http.get(
                f"{config.gamma_host}/events",
                params={
                    "limit": 100,
                    "closed": "false",
                    "order": "id",
                    "ascending": "false",
                }
            )
            events = response.json()
        except Exception as e:
            print(f"[Monitor] Failed to fetch events: {e}")
            return

        new_tokens = []
        seen_market_ids = set()

        for event in events:
            slug = (event.get("slug", "") or "").lower()

            # Only crypto up/down markets (5m, 15m - not 4h)
            if "updown" not in slug:
                continue
            if not any(asset in slug for asset in ["btc", "eth", "xrp", "sol"]):
                continue
            if "4h" in slug:
                continue

            # Extract asset
            asset = "UNK"
            for a in ["BTC", "ETH", "XRP", "SOL"]:
                if a.lower() in slug:
                    asset = a
                    break

            # Extract timeframe
            timeframe = "15m"
            if "5m" in slug:
                timeframe = "5m"

            for market in event.get("markets", []):
                if market.get("closed"):
                    continue
                if not market.get("acceptingOrders", False):
                    continue

                market_id = market.get("conditionId", "")
                if not market_id or market_id in seen_market_ids:
                    continue
                seen_market_ids.add(market_id)

                # Get token IDs
                clob_ids = market.get("clobTokenIds")
                if isinstance(clob_ids, str):
                    try:
                        clob_ids = json.loads(clob_ids)
                    except:
                        continue

                if not clob_ids or len(clob_ids) < 2:
                    continue

                up_token = clob_ids[0]
                down_token = clob_ids[1]

                # Create or update market state
                if market_id not in self._markets:
                    self._markets[market_id] = MarketState(
                        asset=asset,
                        market_id=market_id,
                        timeframe=timeframe,
                        up_token=up_token,
                        down_token=down_token,
                    )
                    new_tokens.extend([up_token, down_token])

                # Map tokens to market
                self._token_to_market[up_token] = market_id
                self._token_to_market[down_token] = market_id

        # Remove stale markets
        stale = [mid for mid in self._markets if mid not in seen_market_ids]
        for mid in stale:
            market = self._markets.pop(mid, None)
            if market:
                self._token_to_market.pop(market.up_token, None)
                self._token_to_market.pop(market.down_token, None)

        print(f"[Monitor] Active markets: {len(self._markets)} | New tokens: {len(new_tokens)}")

        # Subscribe to new tokens
        if new_tokens and self._ws_client:
            await self._ws_client.subscribe(new_tokens)

    async def _snapshot_loop(self):
        """Periodically save snapshots of all market spreads."""
        while self._running:
            try:
                now = datetime.now(timezone.utc)
                if (now - self._last_snapshot).total_seconds() >= self.SNAPSHOT_INTERVAL:
                    await self._save_snapshots()
                    self._last_snapshot = now
            except Exception as e:
                print(f"[Monitor] Snapshot error: {e}")

            await asyncio.sleep(1)

    async def _save_snapshots(self):
        """Save current spreads to database."""
        if not self._markets:
            return

        session = self._Session()
        now = datetime.now(timezone.utc)
        count = 0

        try:
            for market in self._markets.values():
                if not market.has_valid_prices:
                    continue

                snapshot = SpreadSnapshot(
                    timestamp=now,
                    asset=market.asset,
                    market_id=market.market_id,
                    up_ask=market.up_ask,
                    down_ask=market.down_ask,
                    combined=market.combined,
                    spread=market.spread,
                    up_liquidity=market.up_liquidity,
                    down_liquidity=market.down_liquidity,
                    has_opportunity=market.has_opportunity,
                )
                session.add(snapshot)
                count += 1

            session.commit()
            self._snapshots_recorded += count

        except Exception as e:
            print(f"[Monitor] Snapshot DB error: {e}")
            session.rollback()
        finally:
            session.close()

    def get_status(self) -> dict:
        """Get current monitor status."""
        active_opps = sum(
            1 for m in self._markets.values()
            if m.opportunity_start is not None
        )

        return {
            "running": self._running,
            "connected": self._connected,
            "markets_tracked": len(self._markets),
            "tokens_subscribed": self._ws_client.subscribed_count if self._ws_client else 0,
            "snapshots_recorded": self._snapshots_recorded,
            "opportunities_detected": self._opportunities_detected,
            "active_opportunities": active_opps,
        }

    def get_current_spreads(self) -> list[dict]:
        """Get current spread for all tracked markets."""
        spreads = []

        for market in self._markets.values():
            if not market.has_valid_prices:
                continue

            spreads.append({
                "asset": market.asset,
                "timeframe": market.timeframe,
                "up_ask": market.up_ask,
                "down_ask": market.down_ask,
                "combined": market.combined,
                "spread": market.spread,
                "spread_pct": market.spread_pct,
                "has_opportunity": market.has_opportunity,
                "up_liquidity": market.up_liquidity,
                "down_liquidity": market.down_liquidity,
            })

        # Sort by spread descending
        spreads.sort(key=lambda x: x["spread"] or -999, reverse=True)

        return spreads
