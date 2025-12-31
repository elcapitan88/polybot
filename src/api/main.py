"""FastAPI application for Polymarket data dashboard with integrated collector."""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, func, desc
from sqlalchemy.orm import sessionmaker
from pathlib import Path

from src.config import config
from src.models.database import (
    PriceTick, MarketSession, ArbitrageOpportunity, CheapPrice, CollectorStats
)
from src.clients.polymarket import PolymarketClient, Market, MarketPrices
from src.storage.postgres import PostgresStorage

# Global collector state
collector_task = None
collector_running = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start collector on startup, stop on shutdown."""
    global collector_task, collector_running

    print("Starting integrated collector...")
    collector_running = True
    collector_task = asyncio.create_task(run_collector())

    yield

    print("Stopping collector...")
    collector_running = False
    if collector_task:
        collector_task.cancel()
        try:
            await collector_task
        except asyncio.CancelledError:
            pass


app = FastAPI(title="Polymarket Dashboard API", version="1.0.0", lifespan=lifespan)

# CORS - allow all origins for dashboard access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database connection
engine = create_engine(config.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

# Serve static frontend
frontend_path = Path(__file__).parent.parent.parent / "frontend"
if frontend_path.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_path)), name="static")


# ============================================================================
# INTEGRATED COLLECTOR
# ============================================================================

class CollectorState:
    """Track collector statistics."""
    def __init__(self):
        self.total_scans = 0
        self.total_opportunities = 0
        self.total_cheap_prices = 0
        self.start_time = None
        self.last_scan_time = None
        self.active_markets = 0


collector_state = CollectorState()


async def run_collector():
    """Background collector that runs alongside the API."""
    global collector_running

    # Collector settings
    interval_ms = 500
    cheap_threshold = 0.45
    arbitrage_threshold = 0.025
    batch_size = 50

    client = PolymarketClient(mode="read")
    storage = PostgresStorage(config.database_url)

    collector_state.start_time = datetime.utcnow()
    pending_writes = 0

    # Start collector run in database
    run_id = storage.start_collector_run(
        interval_ms=interval_ms,
        cheap_threshold=cheap_threshold,
        arbitrage_threshold=arbitrage_threshold
    )

    print(f"Collector started - scanning every {interval_ms}ms")

    while collector_running:
        try:
            # Get all market prices
            all_prices = client.get_all_market_prices()
            collector_state.active_markets = len(all_prices)
            collector_state.last_scan_time = datetime.utcnow()

            for market, prices in all_prices:
                is_cheap_yes = prices.yes_ask and prices.yes_ask < cheap_threshold
                is_cheap_no = prices.no_ask and prices.no_ask < cheap_threshold

                if is_cheap_yes or is_cheap_no:
                    collector_state.total_cheap_prices += 1

                # Log tick
                storage.log_tick(
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
                pending_writes += 1

                # Log cheap prices
                if is_cheap_yes:
                    storage.log_cheap_price(
                        timestamp=prices.timestamp,
                        market_id=market.condition_id,
                        asset=market.asset,
                        side="YES",
                        price=prices.yes_ask,
                        threshold=cheap_threshold,
                        liquidity=prices.yes_liquidity
                    )
                    pending_writes += 1

                if is_cheap_no:
                    storage.log_cheap_price(
                        timestamp=prices.timestamp,
                        market_id=market.condition_id,
                        asset=market.asset,
                        side="NO",
                        price=prices.no_ask,
                        threshold=cheap_threshold,
                        liquidity=prices.no_liquidity
                    )
                    pending_writes += 1

                # Log arbitrage opportunities
                if prices.has_arbitrage and prices.arbitrage_profit_pct >= arbitrage_threshold:
                    max_profit = 0.0
                    if prices.combined_ask:
                        max_profit = min(prices.yes_liquidity, prices.no_liquidity) * (1.0 - prices.combined_ask)

                    storage.log_opportunity(
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
                    collector_state.total_opportunities += 1
                    pending_writes += 1

            collector_state.total_scans += 1

            # Commit batch
            if pending_writes >= batch_size:
                storage.commit()
                pending_writes = 0

            await asyncio.sleep(interval_ms / 1000.0)

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Collector error: {e}")
            await asyncio.sleep(5)

    # Final commit and cleanup
    if pending_writes > 0:
        storage.commit()

    if run_id:
        storage.update_collector_stats(
            run_id=run_id,
            total_scans=collector_state.total_scans,
            total_opportunities=collector_state.total_opportunities,
            total_cheap_prices=collector_state.total_cheap_prices,
            total_sessions=0
        )
        storage.commit()

    storage.close()
    print("Collector stopped")


@app.get("/")
async def root():
    """Serve the dashboard."""
    index_path = frontend_path / "index.html"
    if index_path.exists():
        return FileResponse(str(index_path))
    return {"message": "Polymarket Dashboard API", "docs": "/docs"}


@app.get("/api/stats")
async def get_stats():
    """Get overall collection statistics."""
    session = SessionLocal()
    try:
        total_ticks = session.query(func.count(PriceTick.id)).scalar() or 0
        total_opportunities = session.query(func.count(ArbitrageOpportunity.id)).scalar() or 0
        total_cheap = session.query(func.count(CheapPrice.id)).scalar() or 0
        total_sessions = session.query(func.count(MarketSession.id)).scalar() or 0

        # Pattern analysis
        sessions_with_data = session.query(MarketSession).filter(
            MarketSession.yes_closed_above_open.isnot(None)
        ).all()

        yes_up_count = sum(1 for s in sessions_with_data if s.yes_closed_above_open)
        no_down_count = sum(1 for s in sessions_with_data if s.no_closed_below_open)

        # Latest tick timestamp
        latest_tick = session.query(PriceTick).order_by(
            desc(PriceTick.timestamp)
        ).first()

        return {
            "total_ticks": total_ticks,
            "total_opportunities": total_opportunities,
            "total_cheap_prices": total_cheap,
            "total_sessions": total_sessions,
            "sessions_analyzed": len(sessions_with_data),
            "yes_closed_above_open": yes_up_count,
            "no_closed_below_open": no_down_count,
            "yes_up_pct": round(yes_up_count / len(sessions_with_data) * 100, 1) if sessions_with_data else 0,
            "no_down_pct": round(no_down_count / len(sessions_with_data) * 100, 1) if sessions_with_data else 0,
            "collector_running": collector_running,
            "collector_scans": collector_state.total_scans,
            "collector_active_markets": collector_state.active_markets,
            "last_tick": latest_tick.timestamp.isoformat() if latest_tick else None,
            "last_scan": collector_state.last_scan_time.isoformat() if collector_state.last_scan_time else None,
        }
    finally:
        session.close()


@app.get("/api/opportunities")
async def get_opportunities(
    limit: int = Query(50, le=500),
    asset: Optional[str] = None,
    min_profit: Optional[float] = None
):
    """Get recent arbitrage opportunities."""
    session = SessionLocal()
    try:
        query = session.query(ArbitrageOpportunity).order_by(
            desc(ArbitrageOpportunity.timestamp)
        )

        if asset:
            query = query.filter(ArbitrageOpportunity.asset == asset.upper())
        if min_profit:
            query = query.filter(ArbitrageOpportunity.profit_pct >= min_profit)

        opportunities = query.limit(limit).all()

        return [
            {
                "id": o.id,
                "timestamp": o.timestamp.isoformat(),
                "asset": o.asset,
                "question": o.question[:100] if o.question else "",
                "yes_ask": o.yes_ask,
                "no_ask": o.no_ask,
                "combined_cost": o.combined_cost,
                "profit_pct": round(o.profit_pct * 100, 2) if o.profit_pct else 0,
                "max_profit_usd": round(o.max_profit_usd, 2) if o.max_profit_usd else 0,
            }
            for o in opportunities
        ]
    finally:
        session.close()


@app.get("/api/cheap-prices")
async def get_cheap_prices(
    limit: int = Query(50, le=500),
    asset: Optional[str] = None,
    side: Optional[str] = None
):
    """Get recent cheap price occurrences."""
    session = SessionLocal()
    try:
        query = session.query(CheapPrice).order_by(desc(CheapPrice.timestamp))

        if asset:
            query = query.filter(CheapPrice.asset == asset.upper())
        if side:
            query = query.filter(CheapPrice.side == side.upper())

        prices = query.limit(limit).all()

        return [
            {
                "id": p.id,
                "timestamp": p.timestamp.isoformat(),
                "asset": p.asset,
                "side": p.side,
                "price": p.price,
                "threshold": p.threshold,
                "liquidity": p.liquidity,
            }
            for p in prices
        ]
    finally:
        session.close()


@app.get("/api/sessions")
async def get_sessions(
    limit: int = Query(50, le=500),
    asset: Optional[str] = None
):
    """Get completed market sessions."""
    session = SessionLocal()
    try:
        query = session.query(MarketSession).order_by(
            desc(MarketSession.close_time)
        )

        if asset:
            query = query.filter(MarketSession.asset == asset.upper())

        sessions = query.limit(limit).all()

        return [
            {
                "id": s.id,
                "market_id": s.market_id,
                "asset": s.asset,
                "question": s.question[:100] if s.question else "",
                "open_time": s.open_time.isoformat() if s.open_time else None,
                "close_time": s.close_time.isoformat() if s.close_time else None,
                "yes_open": s.yes_open,
                "yes_close": s.yes_close,
                "yes_high": s.yes_high,
                "yes_low": s.yes_low,
                "no_open": s.no_open,
                "no_close": s.no_close,
                "no_high": s.no_high,
                "no_low": s.no_low,
                "yes_closed_above_open": s.yes_closed_above_open,
                "no_closed_below_open": s.no_closed_below_open,
                "yes_price_change": round(s.yes_price_change, 4) if s.yes_price_change else None,
                "no_price_change": round(s.no_price_change, 4) if s.no_price_change else None,
                "observation_count": s.observation_count,
                "arbitrage_opportunities": s.arbitrage_opportunities,
            }
            for s in sessions
        ]
    finally:
        session.close()


@app.get("/api/ticks")
async def get_ticks(
    limit: int = Query(100, le=1000),
    asset: Optional[str] = None,
    has_arbitrage: Optional[bool] = None
):
    """Get recent price ticks."""
    session = SessionLocal()
    try:
        query = session.query(PriceTick).order_by(desc(PriceTick.timestamp))

        if asset:
            query = query.filter(PriceTick.asset == asset.upper())
        if has_arbitrage is not None:
            query = query.filter(PriceTick.has_arbitrage == has_arbitrage)

        ticks = query.limit(limit).all()

        return [
            {
                "id": t.id,
                "timestamp": t.timestamp.isoformat(),
                "asset": t.asset,
                "yes_ask": t.yes_ask,
                "no_ask": t.no_ask,
                "combined_cost": t.combined_cost,
                "profit_pct": round(t.profit_pct * 100, 2) if t.profit_pct else None,
                "has_arbitrage": t.has_arbitrage,
            }
            for t in ticks
        ]
    finally:
        session.close()


@app.get("/api/charts/arbitrage-over-time")
async def get_arbitrage_chart(hours: int = Query(24, le=168)):
    """Get arbitrage opportunities over time for charting."""
    session = SessionLocal()
    try:
        since = datetime.utcnow() - timedelta(hours=hours)

        opportunities = session.query(ArbitrageOpportunity).filter(
            ArbitrageOpportunity.timestamp >= since
        ).order_by(ArbitrageOpportunity.timestamp).all()

        return [
            {
                "timestamp": o.timestamp.isoformat(),
                "asset": o.asset,
                "profit_pct": round(o.profit_pct * 100, 2) if o.profit_pct else 0,
            }
            for o in opportunities
        ]
    finally:
        session.close()


@app.get("/api/charts/price-distribution")
async def get_price_distribution(asset: Optional[str] = None, hours: int = Query(24, le=168)):
    """Get price distribution for charting."""
    session = SessionLocal()
    try:
        since = datetime.utcnow() - timedelta(hours=hours)

        query = session.query(PriceTick).filter(PriceTick.timestamp >= since)

        if asset:
            query = query.filter(PriceTick.asset == asset.upper())

        ticks = query.all()

        # Group by price ranges
        yes_prices = [t.yes_ask for t in ticks if t.yes_ask]
        no_prices = [t.no_ask for t in ticks if t.no_ask]

        def bucket_prices(prices):
            buckets = {f"{i/100:.2f}-{(i+5)/100:.2f}": 0 for i in range(0, 100, 5)}
            for p in prices:
                bucket_idx = int(p * 100) // 5 * 5
                bucket_key = f"{bucket_idx/100:.2f}-{(bucket_idx+5)/100:.2f}"
                if bucket_key in buckets:
                    buckets[bucket_key] += 1
            return buckets

        return {
            "yes_distribution": bucket_prices(yes_prices),
            "no_distribution": bucket_prices(no_prices),
            "total_ticks": len(ticks),
        }
    finally:
        session.close()
