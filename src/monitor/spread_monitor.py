"""
Spread Monitor for Polymarket Delta-Neutral Strategy.

Monitors 15m crypto markets and detects when combined UP + DOWN < $1.00.
Uses last-trade-price polling for accurate pricing.
"""

import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Optional
from dataclasses import dataclass
import httpx

from src.config import config
from src.models.database import (
    init_db, SpreadSnapshot, Opportunity, MarketWindow,
    DailyStats, MonitorStatus
)


@dataclass
class MarketState:
    """Current state of a single market (UP/DOWN pair)."""
    asset: str
    market_id: str
    timeframe: str  # "15m" only

    # Token IDs
    up_token: str
    down_token: str

    # Current prices (last trade prices)
    up_price: Optional[float] = None
    down_price: Optional[float] = None

    # Timestamps
    up_updated: Optional[datetime] = None
    down_updated: Optional[datetime] = None

    # Opportunity tracking
    opportunity_start: Optional[datetime] = None
    opportunity_best_spread: float = 0.0

    @property
    def combined(self) -> Optional[float]:
        """Combined cost to buy both sides."""
        if self.up_price and self.down_price:
            return self.up_price + self.down_price
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
        if self.up_price is None or self.down_price is None:
            return False
        # Filter out extreme prices
        if self.up_price <= 0.01 or self.down_price <= 0.01:
            return False
        if self.up_price >= 0.99 or self.down_price >= 0.99:
            return False
        return True


class SpreadMonitor:
    """
    Main monitoring system for Polymarket spreads.

    - Discovers active 15m crypto markets
    - Polls last-trade-price for real pricing
    - Detects and records opportunities (combined < $1.00)
    - Stores snapshots for historical analysis
    """

    # How often to poll prices (seconds)
    POLL_INTERVAL = 1.0

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

        # HTTP client
        self._http: Optional[httpx.AsyncClient] = None

        # State
        self._running = False
        self._connected = False
        self._last_snapshot = datetime.now(timezone.utc)
        self._last_market_refresh = datetime.min.replace(tzinfo=timezone.utc)

        # Stats
        self._snapshots_recorded = 0
        self._opportunities_detected = 0
        self._polls_completed = 0

    async def start(self):
        """Start the spread monitor."""
        print("[Monitor] Starting spread monitor...")
        self._running = True
        self._http = httpx.AsyncClient(timeout=30)

        # Initial market discovery
        await self._refresh_markets()

        # Start tasks
        await asyncio.gather(
            self._price_poll_loop(),
            self._market_refresh_loop(),
            self._snapshot_loop(),
            return_exceptions=True,
        )

    async def stop(self):
        """Stop the spread monitor."""
        print("[Monitor] Stopping...")
        self._running = False

        if self._http:
            await self._http.aclose()
        print("[Monitor] Stopped")

    async def _price_poll_loop(self):
        """Continuously poll prices for all markets."""
        while self._running:
            try:
                await self._poll_all_prices()
                self._polls_completed += 1
                self._connected = True
            except Exception as e:
                print(f"[Monitor] Poll error: {e}")
                self._connected = False

            await asyncio.sleep(self.POLL_INTERVAL)

    async def _poll_all_prices(self):
        """Poll last-trade-price for all tracked markets."""
        if not self._markets or not self._http:
            return

        now = datetime.now(timezone.utc)

        # Poll all tokens concurrently
        tasks = []
        for market in self._markets.values():
            tasks.append(self._poll_token_price(market.up_token, market, "up"))
            tasks.append(self._poll_token_price(market.down_token, market, "down"))

        await asyncio.gather(*tasks, return_exceptions=True)

        # Check for opportunities
        for market in self._markets.values():
            self._check_opportunity(market)

    async def _poll_token_price(self, token_id: str, market: MarketState, side: str):
        """Poll price for a single token."""
        try:
            response = await self._http.get(
                f"{config.clob_host}/last-trade-price",
                params={"token_id": token_id}
            )

            if response.status_code == 200:
                data = response.json()
                price = float(data.get("price", 0))
                now = datetime.now(timezone.utc)

                if side == "up":
                    market.up_price = price
                    market.up_updated = now
                else:
                    market.down_price = price
                    market.down_updated = now

        except Exception as e:
            pass  # Silently ignore individual poll failures

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
                up_ask=market.up_price,
                down_ask=market.down_price,
                combined=market.combined,
                spread=market.spread or 0,
                spread_pct=market.spread_pct or 0,
                up_liquidity=0,
                down_liquidity=0,
                max_position=0,
            )
            session.add(opp)
            session.commit()

            print(f"[OPPORTUNITY] {market.asset} | "
                  f"UP=${market.up_price:.3f} + DOWN=${market.down_price:.3f} = "
                  f"${market.combined:.3f} | "
                  f"Spread: {market.spread_pct:.2f}%")

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
        """Fetch active 15m markets."""
        print("[Monitor] Refreshing markets...")

        if not self._http:
            return

        try:
            response = await self._http.get(
                f"{config.gamma_host}/events",
                params={
                    "limit": 500,
                    "closed": "false",
                    "order": "startDate",
                    "ascending": "false",
                }
            )
            events = response.json()
        except Exception as e:
            print(f"[Monitor] Failed to fetch events: {e}")
            return

        seen_market_ids = set()

        for event in events:
            slug = (event.get("slug", "") or "").lower()

            # Only 15m crypto up/down markets
            if "-15m-" not in slug:
                continue
            if not any(asset in slug for asset in ["btc", "eth", "xrp", "sol"]):
                continue

            # Extract asset
            asset = "UNK"
            for a in ["BTC", "ETH", "XRP", "SOL"]:
                if a.lower() in slug:
                    asset = a
                    break

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
                        timeframe="15m",
                        up_token=up_token,
                        down_token=down_token,
                    )

        # Remove stale markets
        stale = [mid for mid in self._markets if mid not in seen_market_ids]
        for mid in stale:
            self._markets.pop(mid, None)

        print(f"[Monitor] Active 15m markets: {len(self._markets)}")

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
                    up_ask=market.up_price,
                    down_ask=market.down_price,
                    combined=market.combined,
                    spread=market.spread,
                    up_liquidity=0,
                    down_liquidity=0,
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
            "polls_completed": self._polls_completed,
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
                "up_price": market.up_price,
                "down_price": market.down_price,
                "combined": market.combined,
                "spread": market.spread,
                "spread_pct": market.spread_pct,
                "has_opportunity": market.has_opportunity,
            })

        # Sort by spread descending
        spreads.sort(key=lambda x: x["spread"] or -999, reverse=True)

        return spreads
