"""WebSocket client for real-time Polymarket order book updates."""

import asyncio
import json
from datetime import datetime, timezone
from typing import Callable, Optional
from dataclasses import dataclass, field

import websockets
from websockets.exceptions import ConnectionClosed

from src.config import config


@dataclass
class OrderBookUpdate:
    """Parsed order book update from WebSocket."""
    token_id: str
    timestamp: datetime
    bids: list[tuple[float, float]]  # [(price, size), ...]
    asks: list[tuple[float, float]]  # [(price, size), ...]
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    ask_liquidity: float = 0.0

    def __post_init__(self):
        if self.asks:
            self.best_ask = self.asks[0][0]
            # Sum liquidity at top 3 price levels
            self.ask_liquidity = sum(size for _, size in self.asks[:3])
        if self.bids:
            self.best_bid = self.bids[0][0]


class PolymarketWebSocket:
    """
    WebSocket client for Polymarket CLOB real-time data.

    Subscribes to order book updates for specified tokens and
    calls the provided callback when updates arrive.
    """

    # Polymarket WebSocket endpoints
    WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    def __init__(
        self,
        on_book_update: Callable[[str, OrderBookUpdate], None],
        on_connect: Optional[Callable[[], None]] = None,
        on_disconnect: Optional[Callable[[], None]] = None,
    ):
        """
        Initialize WebSocket client.

        Args:
            on_book_update: Callback when order book updates (token_id, update)
            on_connect: Callback when connected
            on_disconnect: Callback when disconnected
        """
        self.on_book_update = on_book_update
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._subscribed_tokens: set[str] = set()
        self._running = False
        self._reconnect_delay = 1.0
        self._max_reconnect_delay = 60.0

    async def connect(self):
        """Establish WebSocket connection."""
        try:
            self._ws = await websockets.connect(
                self.WS_URL,
                ping_interval=30,
                ping_timeout=10,
                close_timeout=5,
            )
            self._reconnect_delay = 1.0  # Reset on successful connect
            print(f"[WS] Connected to {self.WS_URL}")

            if self.on_connect:
                self.on_connect()

            return True
        except Exception as e:
            print(f"[WS] Connection failed: {e}")
            return False

    async def subscribe(self, token_ids: list[str]):
        """
        Subscribe to order book updates for given tokens.

        Args:
            token_ids: List of CLOB token IDs to subscribe to
        """
        if not self._ws:
            print("[WS] Not connected, cannot subscribe")
            return

        for token_id in token_ids:
            if token_id in self._subscribed_tokens:
                continue

            # Polymarket subscription message format
            sub_msg = {
                "type": "subscribe",
                "channel": "book",
                "asset_id": token_id,
            }

            try:
                await self._ws.send(json.dumps(sub_msg))
                self._subscribed_tokens.add(token_id)
            except Exception as e:
                print(f"[WS] Subscribe error for {token_id[:20]}...: {e}")

        print(f"[WS] Subscribed to {len(self._subscribed_tokens)} tokens")

    async def unsubscribe(self, token_ids: list[str]):
        """Unsubscribe from token updates."""
        if not self._ws:
            return

        for token_id in token_ids:
            if token_id not in self._subscribed_tokens:
                continue

            unsub_msg = {
                "type": "unsubscribe",
                "channel": "book",
                "asset_id": token_id,
            }

            try:
                await self._ws.send(json.dumps(unsub_msg))
                self._subscribed_tokens.discard(token_id)
            except Exception:
                pass

    async def _handle_message(self, message: str):
        """Parse and handle incoming WebSocket message."""
        try:
            data = json.loads(message)

            # Handle different message types
            msg_type = data.get("type", data.get("event_type", ""))

            if msg_type in ("book", "book_update", "snapshot"):
                # Order book update
                token_id = data.get("asset_id", data.get("market", ""))

                if not token_id:
                    return

                # Parse bids and asks
                bids_raw = data.get("bids", [])
                asks_raw = data.get("asks", [])

                bids = []
                asks = []

                for bid in bids_raw:
                    if isinstance(bid, dict):
                        price = float(bid.get("price", 0))
                        size = float(bid.get("size", 0))
                    elif isinstance(bid, list) and len(bid) >= 2:
                        price = float(bid[0])
                        size = float(bid[1])
                    else:
                        continue
                    if price > 0 and size > 0:
                        bids.append((price, size))

                for ask in asks_raw:
                    if isinstance(ask, dict):
                        price = float(ask.get("price", 0))
                        size = float(ask.get("size", 0))
                    elif isinstance(ask, list) and len(ask) >= 2:
                        price = float(ask[0])
                        size = float(ask[1])
                    else:
                        continue
                    if price > 0 and size > 0:
                        asks.append((price, size))

                # Sort: bids descending, asks ascending
                bids.sort(key=lambda x: x[0], reverse=True)
                asks.sort(key=lambda x: x[0])

                update = OrderBookUpdate(
                    token_id=token_id,
                    timestamp=datetime.now(timezone.utc),
                    bids=bids,
                    asks=asks,
                )

                # Call the update handler
                self.on_book_update(token_id, update)

            elif msg_type == "error":
                print(f"[WS] Error: {data.get('message', data)}")

        except json.JSONDecodeError:
            pass
        except Exception as e:
            print(f"[WS] Message handling error: {e}")

    async def _listen(self):
        """Listen for messages from WebSocket."""
        if not self._ws:
            return

        try:
            async for message in self._ws:
                await self._handle_message(message)
        except ConnectionClosed:
            print("[WS] Connection closed")
        except Exception as e:
            print(f"[WS] Listen error: {e}")

    async def run(self):
        """
        Main run loop with auto-reconnection.
        Call this to start the WebSocket client.
        """
        self._running = True

        while self._running:
            # Connect
            connected = await self.connect()

            if connected:
                # Re-subscribe to previously subscribed tokens
                if self._subscribed_tokens:
                    tokens = list(self._subscribed_tokens)
                    self._subscribed_tokens.clear()
                    await self.subscribe(tokens)

                # Listen for messages
                await self._listen()

                # Disconnected
                if self.on_disconnect:
                    self.on_disconnect()

            if not self._running:
                break

            # Reconnect with backoff
            print(f"[WS] Reconnecting in {self._reconnect_delay}s...")
            await asyncio.sleep(self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * 2,
                self._max_reconnect_delay
            )

    async def stop(self):
        """Stop the WebSocket client."""
        self._running = False

        if self._ws:
            await self._ws.close()
            self._ws = None

        print("[WS] Stopped")

    @property
    def is_connected(self) -> bool:
        """Check if WebSocket is connected."""
        return self._ws is not None and self._ws.open

    @property
    def subscribed_count(self) -> int:
        """Number of subscribed tokens."""
        return len(self._subscribed_tokens)
