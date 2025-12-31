"""
Spread Monitor for Polymarket Delta-Neutral Strategy.

Monitors 15m crypto markets via WebSocket and detects when combined UP + DOWN < $1.00.
Uses real-time order book updates from the CLOB WebSocket API.
"""

import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Optional
from dataclasses import dataclass, field
import httpx
import websockets
from websockets.exceptions import ConnectionClosed

from src.config import config
from src.models.database import (
    init_db, SpreadSnapshot, Opportunity, MarketWindow,
    DailyStats, MonitorStatus
)


# WebSocket endpoint for Polymarket CLOB
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


@dataclass
class MarketState:
    """Current state of a single market (UP/DOWN pair)."""
    asset: str
    market_id: str
    timeframe: str  # "15m" only

    # Token IDs
    up_token: str
    down_token: str

    # Current best ask prices (what we'd pay to buy)
    up_ask: Optional[float] = None
    down_ask: Optional[float] = None

    # Available liquidity at best ask
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
        # Filter out extreme prices (likely no real liquidity)
        if self.up_ask <= 0.01 or self.down_ask <= 0.01:
            return False
        if self.up_ask >= 0.99 or self.down_ask >= 0.99:
            return False
        return True

    @property
    def max_position(self) -> float:
        """Maximum position size limited by liquidity on both sides."""
        return min(self.up_liquidity, self.down_liquidity)


class SpreadMonitor:
    """
    Main monitoring system for Polymarket spreads via WebSocket.

    - Discovers active 15m crypto markets via REST API
    - Connects to WebSocket for real-time order book updates
    - Detects and records opportunities (combined < $1.00)
    - Stores snapshots for historical analysis
    """

    # How often to take snapshots (seconds)
    SNAPSHOT_INTERVAL = 30

    # How often to refresh market list (seconds)
    # More frequent since we need to switch markets every 15 minutes
    MARKET_REFRESH_INTERVAL = 30

    # WebSocket ping interval (seconds)
    WS_PING_INTERVAL = 10

    # Minimum spread to consider an opportunity (avoid noise)
    MIN_SPREAD_PCT = 0.5  # 0.5%

    def __init__(self, database_url: str):
        """Initialize the spread monitor."""
        self.database_url = database_url
        self._engine, self._Session = init_db(database_url)

        # Market state tracking
        self._markets: dict[str, MarketState] = {}  # market_id -> state
        self._token_to_market: dict[str, tuple[str, str]] = {}  # token_id -> (market_id, side)

        # HTTP client for market discovery
        self._http: Optional[httpx.AsyncClient] = None

        # WebSocket connection
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

        # State
        self._running = False
        self._connected = False
        self._last_snapshot = datetime.now(timezone.utc)
        self._last_market_refresh = datetime.min.replace(tzinfo=timezone.utc)

        # Stats
        self._snapshots_recorded = 0
        self._opportunities_detected = 0
        self._messages_received = 0

    async def start(self):
        """Start the spread monitor."""
        print("[Monitor] Starting spread monitor with WebSocket...")
        self._running = True
        self._http = httpx.AsyncClient(timeout=30)

        # Initial market discovery
        await self._refresh_markets()

        # Start background tasks
        await asyncio.gather(
            self._websocket_loop(),
            self._market_refresh_loop(),
            self._snapshot_loop(),
            return_exceptions=True,
        )

    async def stop(self):
        """Stop the spread monitor."""
        print("[Monitor] Stopping...")
        self._running = False

        if self._ws:
            await self._ws.close()
        if self._http:
            await self._http.aclose()
        print("[Monitor] Stopped")

    async def _websocket_loop(self):
        """Main WebSocket connection loop with auto-reconnect."""
        while self._running:
            try:
                await self._connect_and_listen()
            except ConnectionClosed as e:
                print(f"[WS] Connection closed: {e}")
                self._connected = False
            except Exception as e:
                print(f"[WS] Error: {e}")
                self._connected = False

            if self._running:
                print("[WS] Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

    async def _connect_and_listen(self):
        """Connect to WebSocket and listen for messages."""
        print(f"[WS] Connecting to {WS_URL}...")

        async with websockets.connect(WS_URL, ping_interval=None) as ws:
            self._ws = ws
            self._connected = True
            print("[WS] Connected!")

            # Subscribe to all tracked tokens
            await self._subscribe_all_tokens()

            # Start ping task
            ping_task = asyncio.create_task(self._ping_loop())

            try:
                async for message in ws:
                    await self._handle_message(message)
            finally:
                ping_task.cancel()

    async def _ping_loop(self):
        """Send PING messages to keep connection alive."""
        while self._running and self._ws:
            try:
                await asyncio.sleep(self.WS_PING_INTERVAL)
                if self._ws and self._ws.open:
                    await self._ws.send("PING")
            except Exception as e:
                print(f"[WS] Ping error: {e}")
                break

    async def _subscribe_all_tokens(self):
        """Subscribe to all tracked token IDs."""
        if not self._ws or not self._markets:
            return

        # Collect all token IDs
        asset_ids = []
        for market in self._markets.values():
            asset_ids.append(market.up_token)
            asset_ids.append(market.down_token)

        if not asset_ids:
            print("[WS] No tokens to subscribe to")
            return

        # Send subscription message
        subscribe_msg = {
            "assets_ids": asset_ids,
            "type": "market"
        }

        await self._ws.send(json.dumps(subscribe_msg))
        print(f"[WS] Subscribed to {len(asset_ids)} tokens ({len(self._markets)} markets)")

    async def _handle_message(self, raw_message: str):
        """Handle incoming WebSocket message."""
        if raw_message == "PONG":
            return

        self._messages_received += 1

        try:
            # Messages can be arrays
            messages = json.loads(raw_message)
            if not isinstance(messages, list):
                messages = [messages]

            for msg in messages:
                event_type = msg.get("event_type")

                if event_type == "book":
                    await self._handle_book_message(msg)
                elif event_type == "price_change":
                    await self._handle_price_change(msg)
                elif event_type == "last_trade_price":
                    await self._handle_last_trade(msg)

        except json.JSONDecodeError:
            pass
        except Exception as e:
            print(f"[WS] Message handling error: {e}")

    async def _handle_book_message(self, msg: dict):
        """Handle full order book update."""
        asset_id = msg.get("asset_id")
        if not asset_id:
            return

        market_info = self._token_to_market.get(asset_id)
        if not market_info:
            return

        market_id, side = market_info
        market = self._markets.get(market_id)
        if not market:
            return

        # Get best ask (lowest sell price) from sells array
        sells = msg.get("sells", [])
        if sells:
            # Sells are typically sorted by price ascending
            best_ask = float(sells[0].get("price", 0))
            best_ask_size = float(sells[0].get("size", 0))

            now = datetime.now(timezone.utc)

            if side == "up":
                market.up_ask = best_ask
                market.up_liquidity = best_ask_size
                market.up_updated = now
            else:
                market.down_ask = best_ask
                market.down_liquidity = best_ask_size
                market.down_updated = now

            # Check for opportunities
            self._check_opportunity(market)

            # Log periodically
            if self._messages_received % 100 == 0:
                print(f"[WS] {market.asset} | UP=${market.up_ask or 0:.3f} DOWN=${market.down_ask or 0:.3f} | "
                      f"Combined=${market.combined or 0:.3f}")

    async def _handle_price_change(self, msg: dict):
        """Handle price change update."""
        # Price changes include best bid/ask updates
        changes = msg.get("changes", [])
        for change in changes:
            asset_id = change.get("asset_id")
            if not asset_id:
                continue

            market_info = self._token_to_market.get(asset_id)
            if not market_info:
                continue

            market_id, side = market_info
            market = self._markets.get(market_id)
            if not market:
                continue

            # Check if this is an ask (sell) side update
            if change.get("side") == "SELL":
                price = float(change.get("price", 0))
                size = float(change.get("size", 0))
                now = datetime.now(timezone.utc)

                # Only update if this could be the new best ask
                # (We'd need the full book to know for sure, but we can use this as a hint)
                if side == "up":
                    if market.up_ask is None or price <= market.up_ask:
                        market.up_ask = price
                        market.up_liquidity = size
                        market.up_updated = now
                else:
                    if market.down_ask is None or price <= market.down_ask:
                        market.down_ask = price
                        market.down_liquidity = size
                        market.down_updated = now

                self._check_opportunity(market)

    async def _handle_last_trade(self, msg: dict):
        """Handle last trade price update."""
        asset_id = msg.get("asset_id")
        if not asset_id:
            return

        market_info = self._token_to_market.get(asset_id)
        if not market_info:
            return

        market_id, side = market_info
        market = self._markets.get(market_id)
        if not market:
            return

        price = float(msg.get("price", 0))
        now = datetime.now(timezone.utc)

        # Last trade price can be used as a reference if we don't have book data
        if side == "up" and market.up_ask is None:
            market.up_ask = price
            market.up_updated = now
        elif side == "down" and market.down_ask is None:
            market.down_ask = price
            market.down_updated = now

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

            print(f"\n{'='*60}")
            print(f"[OPPORTUNITY DETECTED] {market.asset}")
            print(f"  UP Ask:    ${market.up_ask:.4f} (${market.up_liquidity:.0f} available)")
            print(f"  DOWN Ask:  ${market.down_ask:.4f} (${market.down_liquidity:.0f} available)")
            print(f"  Combined:  ${market.combined:.4f}")
            print(f"  Spread:    {market.spread_pct:.2f}%")
            print(f"  Max Size:  ${market.max_position:.0f}")
            print(f"{'='*60}\n")

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
                    old_count = len(self._markets)
                    await self._refresh_markets()
                    new_count = len(self._markets)

                    # Resubscribe if markets changed
                    if new_count != old_count and self._ws and self._ws.open:
                        await self._subscribe_all_tokens()

                    self._last_market_refresh = now
            except Exception as e:
                print(f"[Monitor] Market refresh error: {e}")

            await asyncio.sleep(5)

    def _get_next_resolution_time(self) -> datetime:
        """Get the next 15-minute resolution timestamp."""
        now = datetime.now(timezone.utc)
        # Round up to next 15-minute mark
        minutes = now.minute
        next_quarter = ((minutes // 15) + 1) * 15
        if next_quarter >= 60:
            next_time = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        else:
            next_time = now.replace(minute=next_quarter, second=0, microsecond=0)
        return next_time

    def _extract_resolution_timestamp(self, slug: str) -> Optional[int]:
        """Extract Unix timestamp from market slug like 'btc-updown-15m-1767242700'."""
        try:
            parts = slug.split("-")
            # Last part should be the timestamp
            timestamp = int(parts[-1])
            # Sanity check: should be a reasonable Unix timestamp (after 2020)
            if timestamp > 1577836800:  # Jan 1, 2020
                return timestamp
        except (ValueError, IndexError):
            pass
        return None

    async def _refresh_markets(self):
        """Fetch only the currently active 15m market for each asset."""
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

        # Calculate the next resolution time
        next_resolution = self._get_next_resolution_time()
        next_resolution_ts = int(next_resolution.timestamp())

        print(f"[Monitor] Looking for markets resolving at {next_resolution.strftime('%H:%M:%S')} UTC (ts={next_resolution_ts})")

        seen_market_ids = set()
        new_token_map = {}
        active_by_asset = {}  # asset -> best matching market

        for event in events:
            slug = (event.get("slug", "") or "").lower()

            # Only 15m crypto up/down markets
            if "-15m-" not in slug:
                continue
            if not any(asset in slug for asset in ["btc", "eth", "xrp", "sol"]):
                continue

            # Extract resolution timestamp from slug
            resolution_ts = self._extract_resolution_timestamp(slug)
            if resolution_ts is None:
                continue

            # Only include markets that resolve at the next 15-minute mark
            # Allow a small tolerance (within 60 seconds)
            if abs(resolution_ts - next_resolution_ts) > 60:
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

                # Store this as the active market for this asset
                active_by_asset[asset] = {
                    "market_id": market_id,
                    "up_token": up_token,
                    "down_token": down_token,
                    "resolution_ts": resolution_ts,
                }

        # Build the final market list from active markets only
        final_market_ids = set()
        for asset, market_info in active_by_asset.items():
            market_id = market_info["market_id"]
            up_token = market_info["up_token"]
            down_token = market_info["down_token"]

            final_market_ids.add(market_id)
            new_token_map[up_token] = (market_id, "up")
            new_token_map[down_token] = (market_id, "down")

            # Create or update market state
            if market_id not in self._markets:
                self._markets[market_id] = MarketState(
                    asset=asset,
                    market_id=market_id,
                    timeframe="15m",
                    up_token=up_token,
                    down_token=down_token,
                )

            resolution_time = datetime.fromtimestamp(market_info["resolution_ts"], tz=timezone.utc)
            print(f"[Monitor] {asset}: Tracking market resolving at {resolution_time.strftime('%H:%M:%S')} UTC")

        # Update token map
        self._token_to_market = new_token_map

        # Remove stale markets (not in the active set)
        stale = [mid for mid in self._markets if mid not in final_market_ids]
        for mid in stale:
            self._markets.pop(mid, None)

        print(f"[Monitor] Active markets: {len(self._markets)} ({len(self._token_to_market)} tokens)")

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
            "messages_received": self._messages_received,
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
                "up_price": market.up_ask,
                "down_price": market.down_ask,
                "up_liquidity": market.up_liquidity,
                "down_liquidity": market.down_liquidity,
                "combined": market.combined,
                "spread": market.spread,
                "spread_pct": market.spread_pct,
                "has_opportunity": market.has_opportunity,
                "max_position": market.max_position,
            })

        # Sort by spread descending
        spreads.sort(key=lambda x: x["spread"] or -999, reverse=True)

        return spreads
