#!/usr/bin/env python3
"""
Enhanced Data Collection Script for Polymarket 15-Minute Crypto Markets

This script collects comprehensive price data to analyze:
1. Arbitrage opportunities (YES + NO < $1.00)
2. Cheap prices below configurable thresholds
3. Open/Close price patterns (does YES close above open? does NO close below open?)
4. Market lifecycle tracking

Data is stored in PostgreSQL for persistence and analysis.

Usage:
    python scripts/enhanced_collector.py [--interval 500]
"""

import argparse
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.clients.polymarket import PolymarketClient, Market, MarketPrices
from src.config import config
from src.storage.postgres import PostgresStorage


@dataclass
class MarketSession:
    """Tracks a single 15-minute market session from open to close."""
    market_id: str
    asset: str
    question: str

    # Opening prices (first observation)
    open_time: Optional[datetime] = None
    yes_open: Optional[float] = None
    no_open: Optional[float] = None

    # Current/Latest prices
    yes_current: Optional[float] = None
    no_current: Optional[float] = None

    # Price extremes
    yes_high: Optional[float] = None
    yes_low: Optional[float] = None
    no_high: Optional[float] = None
    no_low: Optional[float] = None

    # Closing prices (last observation before resolution)
    close_time: Optional[datetime] = None
    yes_close: Optional[float] = None
    no_close: Optional[float] = None

    # Stats
    observation_count: int = 0
    arbitrage_opportunities: int = 0
    cheap_price_hits: int = 0  # Times price was below threshold

    def update(self, prices: MarketPrices, cheap_threshold: float = 0.45):
        """Update session with new price observation."""
        self.observation_count += 1

        # Set opening prices on first observation
        if self.open_time is None:
            self.open_time = prices.timestamp
            self.yes_open = prices.yes_ask
            self.no_open = prices.no_ask
            self.yes_high = prices.yes_ask
            self.yes_low = prices.yes_ask
            self.no_high = prices.no_ask
            self.no_low = prices.no_ask

        # Update current prices
        self.yes_current = prices.yes_ask
        self.no_current = prices.no_ask

        # Track extremes
        if prices.yes_ask:
            self.yes_high = max(self.yes_high or 0, prices.yes_ask)
            self.yes_low = min(self.yes_low or 1, prices.yes_ask)
        if prices.no_ask:
            self.no_high = max(self.no_high or 0, prices.no_ask)
            self.no_low = min(self.no_low or 1, prices.no_ask)

        # Check for arbitrage
        if prices.has_arbitrage:
            self.arbitrage_opportunities += 1

        # Check for cheap prices
        if prices.yes_ask and prices.yes_ask < cheap_threshold:
            self.cheap_price_hits += 1
        if prices.no_ask and prices.no_ask < cheap_threshold:
            self.cheap_price_hits += 1

        # Update close prices (always the latest)
        self.close_time = prices.timestamp
        self.yes_close = prices.yes_ask
        self.no_close = prices.no_ask

    @property
    def yes_closed_above_open(self) -> Optional[bool]:
        """Did YES close above its opening price?"""
        if self.yes_open and self.yes_close:
            return self.yes_close > self.yes_open
        return None

    @property
    def no_closed_below_open(self) -> Optional[bool]:
        """Did NO close below its opening price?"""
        if self.no_open and self.no_close:
            return self.no_close < self.no_open
        return None

    @property
    def yes_price_change(self) -> Optional[float]:
        """YES price change from open to close."""
        if self.yes_open and self.yes_close:
            return self.yes_close - self.yes_open
        return None

    @property
    def no_price_change(self) -> Optional[float]:
        """NO price change from open to close."""
        if self.no_open and self.no_close:
            return self.no_close - self.no_open
        return None


class EnhancedDataCollector:
    """Enhanced collector with PostgreSQL storage."""

    def __init__(
        self,
        database_url: str,
        interval_ms: int = 500,
        cheap_threshold: float = 0.45,
        arbitrage_threshold: float = 0.025
    ):
        self.interval = interval_ms / 1000.0
        self.cheap_threshold = cheap_threshold
        self.arbitrage_threshold = arbitrage_threshold
        self.client = PolymarketClient(mode="read")
        self.storage = PostgresStorage(database_url)
        self.running = False

        # Track active market sessions
        self.active_sessions: dict[str, MarketSession] = {}
        self.completed_sessions: list[MarketSession] = []

        # Stats
        self.total_scans = 0
        self.total_opportunities = 0
        self.total_cheap_prices = 0
        self.start_time = None
        self.run_id = None

        # Batch settings for database writes
        self.batch_size = 50
        self.pending_writes = 0

    def _log_tick(self, market: Market, prices: MarketPrices):
        """Log a single price tick to PostgreSQL."""
        is_cheap_yes = prices.yes_ask and prices.yes_ask < self.cheap_threshold
        is_cheap_no = prices.no_ask and prices.no_ask < self.cheap_threshold

        if is_cheap_yes or is_cheap_no:
            self.total_cheap_prices += 1

        self.storage.log_tick(
            timestamp=prices.timestamp,
            market_id=market.condition_id,
            asset=market.asset,
            question=market.question[:100] if market.question else "",
            yes_ask=prices.yes_ask,
            no_ask=prices.no_ask,
            combined_cost=prices.combined_ask,
            profit_pct=prices.arbitrage_profit_pct,
            yes_liquidity=prices.yes_liquidity,
            no_liquidity=prices.no_liquidity,
            has_arbitrage=prices.has_arbitrage,
            is_cheap_yes=is_cheap_yes,
            is_cheap_no=is_cheap_no
        )
        self.pending_writes += 1

    def _log_opportunity(self, market: Market, prices: MarketPrices):
        """Log arbitrage opportunity to PostgreSQL."""
        max_profit = 0.0
        if prices.combined_ask:
            max_profit = min(prices.yes_liquidity, prices.no_liquidity) * (1.0 - prices.combined_ask)

        self.storage.log_opportunity(
            timestamp=prices.timestamp,
            market_id=market.condition_id,
            asset=market.asset,
            question=market.question,
            yes_ask=prices.yes_ask,
            no_ask=prices.no_ask,
            combined_cost=prices.combined_ask,
            profit_pct=prices.arbitrage_profit_pct,
            yes_liquidity=prices.yes_liquidity,
            no_liquidity=prices.no_liquidity,
            max_profit_usd=max_profit
        )
        self.pending_writes += 1

    def _log_cheap_price(self, market: Market, prices: MarketPrices, side: str):
        """Log cheap price occurrence to PostgreSQL."""
        price = prices.yes_ask if side == "YES" else prices.no_ask
        liquidity = prices.yes_liquidity if side == "YES" else prices.no_liquidity

        self.storage.log_cheap_price(
            timestamp=prices.timestamp,
            market_id=market.condition_id,
            asset=market.asset,
            side=side,
            price=price,
            threshold=self.cheap_threshold,
            liquidity=liquidity
        )
        self.pending_writes += 1

    def _save_completed_session(self, session: MarketSession):
        """Save a completed session to PostgreSQL."""
        self.storage.save_session(
            market_id=session.market_id,
            asset=session.asset,
            question=session.question[:100] if session.question else "",
            open_time=session.open_time,
            close_time=session.close_time,
            yes_open=session.yes_open,
            yes_close=session.yes_close,
            yes_high=session.yes_high,
            yes_low=session.yes_low,
            no_open=session.no_open,
            no_close=session.no_close,
            no_high=session.no_high,
            no_low=session.no_low,
            yes_closed_above_open=session.yes_closed_above_open,
            no_closed_below_open=session.no_closed_below_open,
            yes_price_change=session.yes_price_change,
            no_price_change=session.no_price_change,
            observation_count=session.observation_count,
            arbitrage_opportunities=session.arbitrage_opportunities,
            cheap_price_hits=session.cheap_price_hits
        )
        self.pending_writes += 1
        self.completed_sessions.append(session)

    def _maybe_commit(self, force: bool = False):
        """Commit pending writes if batch size reached or forced."""
        if force or self.pending_writes >= self.batch_size:
            self.storage.commit()
            self.pending_writes = 0

    def _update_session(self, market: Market, prices: MarketPrices):
        """Update or create a market session."""
        key = market.condition_id

        if key not in self.active_sessions:
            self.active_sessions[key] = MarketSession(
                market_id=market.condition_id,
                asset=market.asset,
                question=market.question
            )

        self.active_sessions[key].update(prices, self.cheap_threshold)

    def _check_expired_sessions(self, current_market_ids: set):
        """Check for sessions that are no longer active (market closed)."""
        expired = []
        for market_id in self.active_sessions:
            if market_id not in current_market_ids:
                expired.append(market_id)

        for market_id in expired:
            session = self.active_sessions.pop(market_id)
            self._save_completed_session(session)
            yes_result = 'UP' if session.yes_closed_above_open else 'DOWN'
            no_result = 'DOWN' if session.no_closed_below_open else 'UP'
            print(f"\n  [Session Complete] {session.asset}: YES {yes_result} | NO {no_result}")

    def _print_status(self, opportunities: list, cheap_count: int):
        """Print current status to console."""
        elapsed = (datetime.utcnow() - self.start_time).total_seconds()
        elapsed_min = max(elapsed / 60, 0.001)

        # Calculate session stats
        sessions_with_data = [s for s in self.completed_sessions if s.yes_closed_above_open is not None]
        yes_up_pct = 0
        no_down_pct = 0
        if sessions_with_data:
            yes_up_pct = sum(1 for s in sessions_with_data if s.yes_closed_above_open) / len(sessions_with_data) * 100
            no_down_pct = sum(1 for s in sessions_with_data if s.no_closed_below_open) / len(sessions_with_data) * 100

        print(f"\r[{datetime.utcnow().strftime('%H:%M:%S')}] ", end="")
        print(f"Scans: {self.total_scans} | ", end="")
        print(f"Arb: {self.total_opportunities} ({self.total_opportunities / elapsed_min:.1f}/min) | ", end="")
        print(f"Cheap(<${self.cheap_threshold}): {self.total_cheap_prices} | ", end="")
        print(f"Sessions: {len(self.completed_sessions)} | ", end="")

        if sessions_with_data:
            print(f"YES^: {yes_up_pct:.0f}% NO-: {no_down_pct:.0f}%", end="")

        if opportunities:
            best = opportunities[0]
            print(f" | Best: {best[0].asset} @ {best[1].arbitrage_profit_pct * 100:.2f}%", end="")

        print("    ", end="", flush=True)

    async def run(self):
        """Main collection loop."""
        self.running = True
        self.start_time = datetime.utcnow()

        # Start collector run in database
        self.run_id = self.storage.start_collector_run(
            interval_ms=int(self.interval * 1000),
            cheap_threshold=self.cheap_threshold,
            arbitrage_threshold=self.arbitrage_threshold
        )

        print("=" * 70)
        print("Enhanced Polymarket Data Collection (PostgreSQL)")
        print("=" * 70)
        print(f"Database: Connected")
        print(f"Scan interval: {self.interval * 1000:.0f}ms")
        print(f"Cheap price threshold: ${self.cheap_threshold}")
        print(f"Min arbitrage profit: {self.arbitrage_threshold * 100:.1f}%")
        print("=" * 70)
        print("Tracking: Arbitrage | Cheap Prices | Open/Close Patterns")
        print("Press Ctrl+C to stop\n")

        while self.running:
            try:
                # Get all market prices
                all_prices = self.client.get_all_market_prices()
                current_market_ids = set()

                for market, prices in all_prices:
                    current_market_ids.add(market.condition_id)

                    # Log tick data
                    self._log_tick(market, prices)

                    # Update session tracking
                    self._update_session(market, prices)

                    # Log cheap prices
                    if prices.yes_ask and prices.yes_ask < self.cheap_threshold:
                        self._log_cheap_price(market, prices, "YES")
                    if prices.no_ask and prices.no_ask < self.cheap_threshold:
                        self._log_cheap_price(market, prices, "NO")

                # Check for expired sessions (markets that closed)
                self._check_expired_sessions(current_market_ids)

                # Find and log opportunities
                opportunities = [
                    (m, p) for m, p in all_prices
                    if p.has_arbitrage and p.arbitrage_profit_pct >= self.arbitrage_threshold
                ]

                self.total_scans += 1
                self.total_opportunities += len(opportunities)

                for market, prices in opportunities:
                    self._log_opportunity(market, prices)

                # Maybe commit to database
                self._maybe_commit()

                # Print status
                cheap_this_scan = sum(
                    1 for _, p in all_prices
                    if (p.yes_ask and p.yes_ask < self.cheap_threshold) or
                       (p.no_ask and p.no_ask < self.cheap_threshold)
                )
                self._print_status(opportunities, cheap_this_scan)

                await asyncio.sleep(self.interval)

            except KeyboardInterrupt:
                print("\n\nStopping...")
                self.running = False
            except Exception as e:
                print(f"\nError: {e}")
                import traceback
                traceback.print_exc()
                await asyncio.sleep(5)

        # Save any remaining active sessions
        for session in self.active_sessions.values():
            self._save_completed_session(session)

        # Final commit
        self._maybe_commit(force=True)

        # Update collector stats
        if self.run_id:
            self.storage.update_collector_stats(
                run_id=self.run_id,
                total_scans=self.total_scans,
                total_opportunities=self.total_opportunities,
                total_cheap_prices=self.total_cheap_prices,
                total_sessions=len(self.completed_sessions)
            )
            self.storage.commit()

        self._print_summary()
        self.storage.close()

    def _print_summary(self):
        """Print comprehensive collection summary."""
        elapsed = (datetime.utcnow() - self.start_time).total_seconds()
        elapsed_min = max(elapsed / 60, 0.001)

        # Calculate pattern stats
        sessions_with_data = [s for s in self.completed_sessions if s.yes_closed_above_open is not None]
        yes_up_count = sum(1 for s in sessions_with_data if s.yes_closed_above_open)
        no_down_count = sum(1 for s in sessions_with_data if s.no_closed_below_open)

        print("\n" + "=" * 70)
        print("Collection Summary")
        print("=" * 70)
        print(f"Duration: {elapsed_min:.1f} minutes")
        print(f"Total scans: {self.total_scans}")
        print()
        print("ARBITRAGE OPPORTUNITIES:")
        print(f"  Total found: {self.total_opportunities}")
        print(f"  Rate: {self.total_opportunities / elapsed_min:.2f} per minute")
        print()
        print(f"CHEAP PRICES (below ${self.cheap_threshold}):")
        print(f"  Total occurrences: {self.total_cheap_prices}")
        print(f"  Rate: {self.total_cheap_prices / elapsed_min:.2f} per minute")
        print()
        print("OPEN/CLOSE PATTERNS:")
        print(f"  Completed sessions: {len(self.completed_sessions)}")
        if sessions_with_data:
            yes_pct = yes_up_count / len(sessions_with_data) * 100
            no_pct = no_down_count / len(sessions_with_data) * 100
            print(f"  YES closed above open: {yes_up_count}/{len(sessions_with_data)} ({yes_pct:.1f}%)")
            print(f"  NO closed below open: {no_down_count}/{len(sessions_with_data)} ({no_pct:.1f}%)")
        print()
        print("DATA STORAGE:")
        print("  All data saved to PostgreSQL")

        # Get overall stats from database
        try:
            stats = self.storage.get_stats_summary()
            print(f"  Total ticks in DB: {stats['total_ticks']}")
            print(f"  Total opportunities in DB: {stats['total_opportunities']}")
            print(f"  Total sessions in DB: {stats['total_sessions']}")
        except Exception:
            pass

        print("=" * 70)


async def main():
    parser = argparse.ArgumentParser(description="Enhanced Polymarket data collection with PostgreSQL")
    parser.add_argument(
        "--interval",
        type=int,
        default=500,
        help="Scan interval in milliseconds (default: 500)",
    )
    parser.add_argument(
        "--cheap-threshold",
        type=float,
        default=0.45,
        help="Price threshold for 'cheap' detection (default: 0.45)",
    )
    parser.add_argument(
        "--min-profit",
        type=float,
        default=0.025,
        help="Minimum arbitrage profit percentage (default: 0.025 = 2.5%%)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Deprecated: data now stored in PostgreSQL",
    )

    args = parser.parse_args()

    # Get database URL from config
    database_url = config.database_url
    print(f"Connecting to database...")

    collector = EnhancedDataCollector(
        database_url=database_url,
        interval_ms=args.interval,
        cheap_threshold=args.cheap_threshold,
        arbitrage_threshold=args.min_profit
    )

    await collector.run()


if __name__ == "__main__":
    asyncio.run(main())
