#!/usr/bin/env python3
"""Clean up ALL fake data from markets that weren't actually trading."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:TluXhOOVkOBzxoQtkQwdidPQadtUTWFv@turntable.proxy.rlwy.net:50564/railway"
)

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

print("=" * 60)
print("CLEANING UP ALL FAKE DATA")
print("=" * 60)

# 1. Delete ALL arbitrage opportunities with unrealistic spreads (< $0.90 combined)
# Real arbitrage would be < $1.00 but not as low as $0.06
print("\n1. Removing fake arbitrage opportunities (combined < $0.90)...")
result = session.execute(text("""
    DELETE FROM arbitrage_opportunities
    WHERE combined_cost < 0.90
"""))
session.commit()
print(f"   Deleted {result.rowcount} fake opportunities")

# 2. Delete fake cheap prices at very low prices (< $0.05) - likely from non-trading markets
print("\n2. Removing fake cheap prices (price < $0.05)...")
result = session.execute(text("""
    DELETE FROM cheap_prices
    WHERE price < 0.05
"""))
session.commit()
print(f"   Deleted {result.rowcount} fake cheap prices")

# 3. Delete price ticks with unrealistic combined costs (< $0.90)
print("\n3. Removing fake price ticks (combined_cost < $0.90)...")
result = session.execute(text("""
    DELETE FROM price_ticks
    WHERE combined_cost < 0.90
"""))
session.commit()
print(f"   Deleted {result.rowcount} fake ticks")

# 4. Show remaining counts
print("\n" + "=" * 60)
print("REMAINING DATA AFTER CLEANUP")
print("=" * 60)
tables = ['price_ticks', 'arbitrage_opportunities', 'cheap_prices']
for table in tables:
    count = session.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
    print(f"  {table}: {count:,} rows")

# 5. Show sample of remaining price ticks
print("\n--- SAMPLE PRICE TICKS (Last 10) ---")
ticks = session.execute(text("""
    SELECT timestamp, asset, yes_ask, no_ask, combined_cost, has_arbitrage
    FROM price_ticks
    ORDER BY timestamp DESC
    LIMIT 10
""")).fetchall()

if ticks:
    for tick in ticks:
        ts, asset, yes, no, combined, has_arb = tick
        arb = "🔥 ARB" if has_arb else ""
        if yes and no and combined:
            print(f"  {ts} | {asset} | YES: ${yes:.3f} | NO: ${no:.3f} | Combined: ${combined:.4f} {arb}")
else:
    print("  No price ticks remaining")

# 6. Show remaining opportunities
print("\n--- REMAINING ARBITRAGE OPPORTUNITIES (All) ---")
opps = session.execute(text("""
    SELECT timestamp, asset, yes_ask, no_ask, combined_cost, profit_pct
    FROM arbitrage_opportunities
    ORDER BY timestamp DESC
    LIMIT 20
""")).fetchall()

if opps:
    for opp in opps:
        ts, asset, yes, no, combined, profit = opp
        spread = 1.0 - combined if combined else 0
        print(f"  {ts} | {asset} | YES: ${yes:.3f} | NO: ${no:.3f} | Spread: ${spread:.4f} | Profit: {profit*100:.1f}%")
else:
    print("  No remaining opportunities - will collect real ones now")

session.close()
print("\n" + "=" * 60)
print("CLEANUP COMPLETE - Ready for clean data collection!")
print("=" * 60)