#!/usr/bin/env python3
"""Analyze collected data in the database."""

import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Use the Railway database URL
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:TluXhOOVkOBzxoQtkQwdidPQadtUTWFv@turntable.proxy.rlwy.net:50564/railway"
)

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

print("=" * 80)
print("DATABASE ANALYSIS")
print("=" * 80)

# 1. Check table counts
print("\n--- TABLE COUNTS ---")
tables = ['price_ticks', 'arbitrage_opportunities', 'cheap_prices', 'market_sessions', 'collector_stats']
for table in tables:
    try:
        result = session.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
        print(f"{table}: {result:,} rows")
    except Exception as e:
        print(f"{table}: ERROR - {e}")

# 2. Analyze arbitrage opportunities
print("\n--- ARBITRAGE OPPORTUNITIES (Last 20) ---")
opps = session.execute(text("""
    SELECT timestamp, asset, question, yes_ask, no_ask, combined_cost, profit_pct, max_profit_usd
    FROM arbitrage_opportunities
    ORDER BY timestamp DESC
    LIMIT 20
""")).fetchall()

if opps:
    for opp in opps:
        ts, asset, question, yes_ask, no_ask, combined, profit_pct, max_profit = opp
        spread = 1.0 - combined if combined else None
        print(f"\n{ts} | {asset}")
        print(f"  Question: {question[:60]}...")
        print(f"  YES Ask: ${yes_ask:.4f} | NO Ask: ${no_ask:.4f}")
        spread_str = f"${spread:.4f}" if spread else "N/A"
        print(f"  Combined: ${combined:.4f} | Spread: {spread_str}")
        print(f"  Profit %: {profit_pct*100:.2f}% | Max Profit: ${max_profit:.2f}")
else:
    print("No opportunities found!")

# 3. Check for suspicious data (both sides very low)
print("\n--- SUSPICIOUS DATA CHECK (Both sides < $0.10) ---")
suspicious = session.execute(text("""
    SELECT COUNT(*) FROM price_ticks
    WHERE yes_ask < 0.10 AND no_ask < 0.10
""")).scalar()
total_ticks = session.execute(text("SELECT COUNT(*) FROM price_ticks")).scalar()
print(f"Ticks where both YES and NO < $0.10: {suspicious:,} out of {total_ticks:,}")

# 4. Price distribution
print("\n--- PRICE DISTRIBUTION (YES side) ---")
distribution = session.execute(text("""
    SELECT
        CASE
            WHEN yes_ask < 0.10 THEN '0.00-0.10'
            WHEN yes_ask < 0.20 THEN '0.10-0.20'
            WHEN yes_ask < 0.30 THEN '0.20-0.30'
            WHEN yes_ask < 0.40 THEN '0.30-0.40'
            WHEN yes_ask < 0.50 THEN '0.40-0.50'
            WHEN yes_ask < 0.60 THEN '0.50-0.60'
            WHEN yes_ask < 0.70 THEN '0.60-0.70'
            WHEN yes_ask < 0.80 THEN '0.70-0.80'
            WHEN yes_ask < 0.90 THEN '0.80-0.90'
            ELSE '0.90-1.00'
        END as price_range,
        COUNT(*) as count
    FROM price_ticks
    WHERE yes_ask IS NOT NULL
    GROUP BY price_range
    ORDER BY price_range
""")).fetchall()
for row in distribution:
    print(f"  {row[0]}: {row[1]:,}")

# 5. Check combined costs
print("\n--- COMBINED COST DISTRIBUTION ---")
combined_dist = session.execute(text("""
    SELECT
        CASE
            WHEN combined_cost < 0.90 THEN '< $0.90 (ARBIT!)'
            WHEN combined_cost < 0.95 THEN '$0.90-0.95'
            WHEN combined_cost < 0.98 THEN '$0.95-0.98'
            WHEN combined_cost < 1.00 THEN '$0.98-1.00'
            WHEN combined_cost < 1.02 THEN '$1.00-1.02'
            ELSE '> $1.02'
        END as range,
        COUNT(*) as count
    FROM price_ticks
    WHERE combined_cost IS NOT NULL
    GROUP BY range
    ORDER BY range
""")).fetchall()
for row in combined_dist:
    print(f"  {row[0]}: {row[1]:,}")

# 6. Recent ticks sample
print("\n--- RECENT PRICE TICKS (Last 10) ---")
ticks = session.execute(text("""
    SELECT timestamp, asset, yes_ask, no_ask, combined_cost, has_arbitrage
    FROM price_ticks
    ORDER BY timestamp DESC
    LIMIT 10
""")).fetchall()
for tick in ticks:
    ts, asset, yes, no, combined, has_arb = tick
    arb_flag = "🔥 ARB" if has_arb else ""
    print(f"  {ts} | {asset:6s} | YES: ${yes:.4f} | NO: ${no:.4f} | Combined: ${combined:.4f} {arb_flag}")

# 7. Unique assets/markets
print("\n--- UNIQUE ASSETS BEING TRACKED ---")
assets = session.execute(text("""
    SELECT DISTINCT asset FROM price_ticks ORDER BY asset
""")).fetchall()
print(f"Total unique assets: {len(assets)}")
for asset in assets[:20]:
    print(f"  - {asset[0]}")
if len(assets) > 20:
    print(f"  ... and {len(assets) - 20} more")

# 8. Check the actual arbitrage detection logic
print("\n--- VERIFYING ARBITRAGE LOGIC ---")
# Get ticks marked as having arbitrage
arb_ticks = session.execute(text("""
    SELECT timestamp, asset, yes_ask, no_ask, combined_cost, profit_pct
    FROM price_ticks
    WHERE has_arbitrage = true
    ORDER BY timestamp DESC
    LIMIT 10
""")).fetchall()
print("Ticks marked as arbitrage:")
for tick in arb_ticks:
    ts, asset, yes, no, combined, profit = tick
    actual_combined = yes + no if yes and no else None
    print(f"  {asset}: YES ${yes:.4f} + NO ${no:.4f} = ${actual_combined:.4f} (stored: ${combined:.4f})")

session.close()
print("\n" + "=" * 80)
