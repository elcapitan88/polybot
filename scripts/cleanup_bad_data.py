#!/usr/bin/env python3
"""Clean up fake arbitrage opportunities (min-tick prices with no liquidity)."""

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
print("CLEANING UP BAD DATA")
print("=" * 60)

# 1. Delete fake arbitrage opportunities (both prices at $0.02)
print("\n1. Removing fake arbitrage opportunities (YES=$0.02 AND NO=$0.02)...")
result = session.execute(text("""
    DELETE FROM arbitrage_opportunities
    WHERE yes_ask = 0.02 AND no_ask = 0.02
"""))
session.commit()
print(f"   Deleted {result.rowcount} fake opportunities")

# 2. Delete fake cheap prices at minimum tick with low liquidity
print("\n2. Removing fake cheap prices (price=$0.02 with liquidity < 10)...")
result = session.execute(text("""
    DELETE FROM cheap_prices
    WHERE price = 0.02 AND liquidity < 10
"""))
session.commit()
print(f"   Deleted {result.rowcount} fake cheap prices")

# 3. Delete price ticks with both sides at minimum tick
print("\n3. Removing fake price ticks (YES=$0.02 AND NO=$0.02)...")
result = session.execute(text("""
    DELETE FROM price_ticks
    WHERE yes_ask = 0.02 AND no_ask = 0.02
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

# 5. Show sample of remaining opportunities
print("\n--- REMAINING ARBITRAGE OPPORTUNITIES (Top 10) ---")
opps = session.execute(text("""
    SELECT timestamp, asset, yes_ask, no_ask, combined_cost, profit_pct, yes_liquidity, no_liquidity
    FROM arbitrage_opportunities
    ORDER BY timestamp DESC
    LIMIT 10
""")).fetchall()

if opps:
    for opp in opps:
        ts, asset, yes, no, combined, profit, yes_liq, no_liq = opp
        spread = 1.0 - combined if combined else 0
        print(f"  {ts} | {asset} | YES: ${yes:.3f} | NO: ${no:.3f} | Spread: ${spread:.4f} | Liq: {yes_liq:.0f}/{no_liq:.0f}")
else:
    print("  No remaining opportunities (good - we'll start collecting real ones now)")

session.close()
print("\n" + "=" * 60)
print("CLEANUP COMPLETE")
print("=" * 60)
