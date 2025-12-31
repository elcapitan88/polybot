#!/usr/bin/env python3
"""Debug the Polymarket API to understand what prices are being returned."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.clients.polymarket import PolymarketClient

client = PolymarketClient(mode="read")

print("=" * 80)
print("POLYMARKET API DEBUG")
print("=" * 80)

# Get active markets
markets = client.get_active_15min_markets()
print(f"\nFound {len(markets)} active 5m/15m markets")

for market in markets[:10]:  # Check first 10
    print(f"\n{'='*60}")
    print(f"Market: {market.question[:60]}...")
    print(f"Asset: {market.asset}")
    print(f"YES Token: {market.yes_token_id}")
    print(f"NO Token: {market.no_token_id}")

    # Get prices using different methods
    print("\n--- PRICES ---")

    # Method 1: /price endpoint
    yes_buy = client.get_token_price(market.yes_token_id, "buy")
    yes_sell = client.get_token_price(market.yes_token_id, "sell")
    no_buy = client.get_token_price(market.no_token_id, "buy")
    no_sell = client.get_token_price(market.no_token_id, "sell")

    print(f"  /price endpoint:")
    print(f"    YES - Buy: ${yes_buy} | Sell: ${yes_sell}")
    print(f"    NO  - Buy: ${no_buy} | Sell: ${no_sell}")
    if yes_buy and no_buy:
        combined = yes_buy + no_buy
        print(f"    Combined (buy YES + buy NO): ${combined:.4f}")
        if combined < 1.0:
            print(f"    ⚠️  ARBITRAGE? Profit = ${1-combined:.4f} ({(1-combined)/combined*100:.1f}%)")

    # Method 2: Order book
    yes_book = client.get_orderbook(market.yes_token_id)
    no_book = client.get_orderbook(market.no_token_id)

    print(f"\n  Order Book:")
    print(f"    YES - Best Ask: ${yes_book['best_ask']} | Best Bid: ${yes_book['best_bid']} | Ask Liq: {yes_book['ask_liquidity']:.0f}")
    print(f"    NO  - Best Ask: ${no_book['best_ask']} | Best Bid: ${no_book['best_bid']} | Ask Liq: {no_book['ask_liquidity']:.0f}")

    if yes_book['best_ask'] and no_book['best_ask']:
        book_combined = float(yes_book['best_ask']) + float(no_book['best_ask'])
        print(f"    Combined (ask+ask): ${book_combined:.4f}")
        if book_combined < 1.0:
            print(f"    ⚠️  BOOK ARBITRAGE? Profit = ${1-book_combined:.4f}")

    # Show full order book
    print(f"\n  YES Order Book (top 3):")
    for i, (price, size) in enumerate(yes_book['asks'][:3]):
        print(f"    Ask {i+1}: ${price:.4f} x {size:.0f}")
    for i, (price, size) in enumerate(yes_book['bids'][:3]):
        print(f"    Bid {i+1}: ${price:.4f} x {size:.0f}")

    print(f"\n  NO Order Book (top 3):")
    for i, (price, size) in enumerate(no_book['asks'][:3]):
        print(f"    Ask {i+1}: ${price:.4f} x {size:.0f}")
    for i, (price, size) in enumerate(no_book['bids'][:3]):
        print(f"    Bid {i+1}: ${price:.4f} x {size:.0f}")

client.close()
print("\n" + "=" * 80)
