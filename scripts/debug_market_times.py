#!/usr/bin/env python3
"""Debug market times to understand which markets are currently active."""

import os
import sys
from datetime import datetime, timezone
import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import config

http = httpx.Client(timeout=30)

print("=" * 80)
print("MARKET TIME DEBUG")
print(f"Current UTC time: {datetime.now(timezone.utc)}")
print("=" * 80)

# Get events from Gamma API
response = http.get(
    f"{config.gamma_host}/events",
    params={
        "limit": 50,
        "order": "id",
        "ascending": "false",
        "closed": "false",
    },
)
events = response.json()

for event in events[:20]:
    title = event.get("title", "")
    slug = event.get("slug", "")

    # Only look at crypto up/down events
    if "updown" not in slug.lower():
        continue
    if not any(asset in slug.lower() for asset in ["btc", "eth", "xrp", "sol"]):
        continue
    if "4h" in slug.lower():  # Skip 4-hour markets
        continue

    print(f"\n{'='*60}")
    print(f"Event: {title}")
    print(f"Slug: {slug}")

    for market in event.get("markets", []):
        if market.get("closed"):
            continue

        question = market.get("question", "")
        end_date_str = market.get("endDate")
        start_date_str = market.get("startDate")
        active = market.get("active")

        print(f"\n  Market: {question[:60]}...")
        print(f"  Active flag: {active}")
        print(f"  Start: {start_date_str}")
        print(f"  End: {end_date_str}")

        # Parse dates
        if end_date_str:
            try:
                end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                if now > end_date:
                    print(f"  ❌ EXPIRED (ended {(now - end_date).total_seconds()/60:.0f} min ago)")
                else:
                    minutes_left = (end_date - now).total_seconds() / 60
                    print(f"  ⏰ {minutes_left:.0f} minutes until close")
            except:
                pass

        if start_date_str:
            try:
                start_date = datetime.fromisoformat(start_date_str.replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                if now < start_date:
                    minutes_until = (start_date - now).total_seconds() / 60
                    print(f"  ⏳ NOT YET STARTED - starts in {minutes_until:.0f} minutes")
                else:
                    print(f"  ✅ STARTED (began {(now - start_date).total_seconds()/60:.0f} min ago)")
            except:
                pass

        # Check all available fields
        print(f"  All fields: {list(market.keys())}")

http.close()
