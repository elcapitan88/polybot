#!/usr/bin/env python3
"""
Data Collection Script for Polymarket 15-Minute Crypto Markets

This script collects price data from Polymarket to analyze:
1. How often arbitrage opportunities appear
2. How large they are (profit percentage)
3. How long they last
4. What time of day they appear most

Run this for at least 24-48 hours before building trading logic.

Usage:
    python scripts/collect_data.py [--interval 500] [--output data/prices.csv]
"""

import argparse
import asyncio
import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.clients.polymarket import PolymarketClient
from src.config import config


class DataCollector:
    """Collects and logs price data from Polymarket."""

    def __init__(self, output_file: str, interval_ms: int = 500):
        self.output_file = output_file
        self.interval = interval_ms / 1000.0  # Convert to seconds
        self.client = PolymarketClient(mode="read")
        self.running = False

        # Stats
        self.total_scans = 0
        self.total_opportunities = 0
        self.start_time = None

        # Ensure output directory exists
        Path(output_file).parent.mkdir(parents=True, exist_ok=True)

        # Initialize CSV file
        self._init_csv()

    def _init_csv(self):
        """Initialize CSV file with headers."""
        file_exists = Path(self.output_file).exists()

        if not file_exists:
            with open(self.output_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "timestamp",
                        "market_id",
                        "asset",
                        "question",
                        "yes_ask",
                        "no_ask",
                        "combined_cost",
                        "profit_pct",
                        "yes_liquidity",
                        "no_liquidity",
                        "has_arbitrage",
                    ]
                )

    def _log_prices(self, market, prices):
        """Log prices to CSV."""
        with open(self.output_file, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    prices.timestamp.isoformat(),
                    market.condition_id,
                    market.asset,
                    market.question[:100],  # Truncate long questions
                    prices.yes_ask,
                    prices.no_ask,
                    prices.combined_ask,
                    prices.arbitrage_profit_pct,
                    prices.yes_liquidity,
                    prices.no_liquidity,
                    prices.has_arbitrage,
                ]
            )

    def _print_status(self, opportunities: list):
        """Print current status to console."""
        elapsed = (datetime.utcnow() - self.start_time).total_seconds()
        elapsed_min = elapsed / 60

        print(f"\r[{datetime.utcnow().strftime('%H:%M:%S')}] ", end="")
        print(f"Scans: {self.total_scans} | ", end="")
        print(f"Opportunities: {self.total_opportunities} | ", end="")
        print(f"Rate: {self.total_opportunities / elapsed_min:.1f}/min | ", end="")

        if opportunities:
            best = opportunities[0]
            print(
                f"Best: {best[0].asset} @ {best[1].arbitrage_profit_pct * 100:.2f}%",
                end="",
            )
        else:
            print("No current opportunities", end="")

        print("    ", end="", flush=True)

    async def run(self):
        """Main collection loop."""
        self.running = True
        self.start_time = datetime.utcnow()

        print("=" * 60)
        print("Polymarket Data Collection")
        print("=" * 60)
        print(f"Output file: {self.output_file}")
        print(f"Scan interval: {self.interval * 1000:.0f}ms")
        print(f"Min profit threshold: {config.min_profit_pct * 100:.1f}%")
        print("=" * 60)
        print("Press Ctrl+C to stop\n")

        while self.running:
            try:
                # Get all market prices
                all_prices = self.client.get_all_market_prices()

                # Log all prices
                for market, prices in all_prices:
                    self._log_prices(market, prices)

                # Find opportunities
                opportunities = [
                    (m, p)
                    for m, p in all_prices
                    if p.has_arbitrage and p.arbitrage_profit_pct >= config.min_profit_pct
                ]

                self.total_scans += 1
                self.total_opportunities += len(opportunities)

                # Print status
                self._print_status(opportunities)

                # Log opportunities to separate file if found
                if opportunities:
                    self._log_opportunities(opportunities)

                await asyncio.sleep(self.interval)

            except KeyboardInterrupt:
                print("\n\nStopping...")
                self.running = False
            except Exception as e:
                print(f"\nError: {e}")
                await asyncio.sleep(5)

        self._print_summary()

    def _log_opportunities(self, opportunities: list):
        """Log opportunities to separate JSON file."""
        opp_file = self.output_file.replace(".csv", "_opportunities.jsonl")

        with open(opp_file, "a") as f:
            for market, prices in opportunities:
                record = {
                    "timestamp": prices.timestamp.isoformat(),
                    "market_id": market.condition_id,
                    "asset": market.asset,
                    "question": market.question,
                    "yes_ask": prices.yes_ask,
                    "no_ask": prices.no_ask,
                    "combined_cost": prices.combined_ask,
                    "profit_pct": prices.arbitrage_profit_pct,
                    "yes_liquidity": prices.yes_liquidity,
                    "no_liquidity": prices.no_liquidity,
                    "max_profit_usd": min(prices.yes_liquidity, prices.no_liquidity)
                    * (1.0 - prices.combined_ask),
                }
                f.write(json.dumps(record) + "\n")

    def _print_summary(self):
        """Print collection summary."""
        elapsed = (datetime.utcnow() - self.start_time).total_seconds()
        elapsed_min = elapsed / 60

        print("\n" + "=" * 60)
        print("Collection Summary")
        print("=" * 60)
        print(f"Duration: {elapsed_min:.1f} minutes")
        print(f"Total scans: {self.total_scans}")
        print(f"Total opportunities: {self.total_opportunities}")
        print(f"Opportunity rate: {self.total_opportunities / elapsed_min:.2f} per minute")
        print(f"Data saved to: {self.output_file}")
        print("=" * 60)


async def main():
    parser = argparse.ArgumentParser(description="Collect Polymarket price data")
    parser.add_argument(
        "--interval",
        type=int,
        default=500,
        help="Scan interval in milliseconds (default: 500)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="data/prices.csv",
        help="Output CSV file path (default: data/prices.csv)",
    )

    args = parser.parse_args()

    collector = DataCollector(output_file=args.output, interval_ms=args.interval)

    await collector.run()


if __name__ == "__main__":
    asyncio.run(main())
