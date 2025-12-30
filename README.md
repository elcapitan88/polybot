# Polymarket 15-Minute Crypto Arbitrage Bot

An automated arbitrage bot for Polymarket's 15-minute cryptocurrency prediction markets.

## Strategy Overview

This bot exploits pricing inefficiencies in Polymarket's binary crypto markets:

- **Markets**: BTC, ETH, XRP, SOL "Up or Down" 15-minute predictions
- **Strategy**: Buy both YES and NO when combined cost < $1.00
- **Edge**: Guaranteed profit when `YES_price + NO_price < $1.00`
- **Example**: Buy YES @ $0.48 + NO @ $0.49 = $0.97 cost → $1.00 payout = 3.1% profit

## Quick Start

### Phase 1: Data Collection (No trading, just research)

```bash
# 1. Clone and setup
git clone <repo>
cd polymarket-bot

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -e .

# 4. Copy environment file
cp .env.example .env

# 5. Run data collection (no wallet needed)
python scripts/enhanced_collector.py --interval 500 --output data/prices.csv
```

This tracks:
- **Arbitrage opportunities** (YES + NO < $1.00)
- **Cheap prices** below configurable threshold (default $0.45)
- **Open/Close patterns** (does YES close above open? does NO close below open?)

Let this run for 24-48 hours to understand:
- How often opportunities appear
- What profit percentages are realistic
- When opportunities are most frequent
- Price movement patterns across 15-min windows

### Railway Deployment (Cloud)

```bash
# 1. Install Railway CLI
npm install -g @railway/cli

# 2. Login to Railway
railway login

# 3. Create new project
railway init

# 4. Deploy
railway up

# 5. View logs
railway logs
```

Railway will automatically detect the Python project and deploy. Data is saved to `/app/data/` on the container.

### Phase 2: Paper Trading (Coming Soon)

```bash
# Set mode to paper in .env
MODE=paper

# Run bot
python -m src.main
```

### Phase 3: Live Trading (Coming Soon)

```bash
# Add your Polygon wallet to .env
POLYGON_PRIVATE_KEY=0x...
POLYGON_WALLET_ADDRESS=0x...
MODE=live

# Fund wallet with USDC on Polygon
# Run bot
python -m src.main
```

## Project Structure

```
polymarket-bot/
├── src/
│   ├── clients/          # API clients (Polymarket, WebSocket)
│   ├── services/         # Core logic (scanner, executor, risk)
│   ├── models/           # Data models
│   ├── storage/          # Database & cache
│   └── config.py         # Configuration
├── scripts/
│   └── collect_data.py   # Data collection script
├── data/                 # Collected data (gitignored)
├── tests/                # Test suite
└── docker-compose.yml    # Local services (Redis, Postgres)
```

## Configuration

Key settings in `.env`:

| Variable | Description | Default |
|----------|-------------|---------|
| `MODE` | `paper` or `live` | `paper` |
| `MIN_PROFIT_PCT` | Minimum profit to trade | `0.025` (2.5%) |
| `MAX_POSITION_USD` | Max USD per trade | `50` |
| `MAX_DAILY_LOSS` | Stop if daily loss exceeds | `100` |
| `SCAN_INTERVAL_MS` | Milliseconds between scans | `500` |

## Risk Warning

⚠️ **Important Disclaimers**:

1. **83% of Polymarket traders lose money** - Most edge gets competed away
2. **Execution risk** - Partial fills create directional exposure
3. **Regulatory risk** - US residents face legal ambiguity
4. **Start small** - Test with minimum capital first

## Dependencies

- Python 3.11+
- `py-clob-client` - Official Polymarket SDK
- `websockets` - Real-time data feeds
- `redis` - Caching (optional)
- `postgresql` - Historical data (optional)

## License

MIT

## Disclaimer

This software is for educational purposes only. Trading involves risk. Past performance does not guarantee future results. Not financial advice.
