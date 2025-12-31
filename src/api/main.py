"""
FastAPI application for Polymarket Spread Monitor.

Provides REST API for:
- Real-time spread monitoring status
- Historical opportunity data
- Statistics and analytics
"""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from src.config import config
from src.monitor.spread_monitor import SpreadMonitor
from src.storage.postgres import Storage

# Global monitor instance
monitor: Optional[SpreadMonitor] = None
storage: Optional[Storage] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start monitor on startup, stop on shutdown."""
    global monitor, storage

    print("[API] Starting Spread Monitor...")

    storage = Storage(config.database_url)
    monitor = SpreadMonitor(config.database_url)

    # Start monitor in background
    monitor_task = asyncio.create_task(monitor.start())

    yield

    # Shutdown
    print("[API] Shutting down...")
    if monitor:
        await monitor.stop()


app = FastAPI(
    title="Polymarket Spread Monitor",
    description="Real-time monitoring for delta-neutral arbitrage opportunities",
    version="2.0.0",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# Health & Status
# =============================================================================

@app.get("/")
async def root():
    """Root endpoint with basic info."""
    return {
        "service": "Polymarket Spread Monitor",
        "version": "2.0.0",
        "status": "running" if monitor else "starting",
        "docs": "/docs",
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}


@app.get("/api/status")
async def get_status():
    """Get current monitor status."""
    if not monitor:
        return {"error": "Monitor not initialized"}

    status = monitor.get_status()

    # Add storage stats
    if storage:
        stats = storage.get_opportunity_stats(hours=24)
        status["last_24h"] = stats

    return status


# =============================================================================
# Real-time Data
# =============================================================================

@app.get("/api/spreads")
async def get_current_spreads():
    """Get current spreads for all tracked markets."""
    if not monitor:
        return {"error": "Monitor not initialized", "spreads": []}

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "spreads": monitor.get_current_spreads(),
    }


@app.get("/api/spreads/{asset}")
async def get_asset_spreads(asset: str):
    """Get current spreads for a specific asset."""
    if not monitor:
        return {"error": "Monitor not initialized", "spreads": []}

    all_spreads = monitor.get_current_spreads()
    filtered = [s for s in all_spreads if s["asset"].upper() == asset.upper()]

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "asset": asset.upper(),
        "spreads": filtered,
    }


# =============================================================================
# Opportunities
# =============================================================================

@app.get("/api/opportunities")
async def get_opportunities(
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(50, ge=1, le=500),
    asset: Optional[str] = None,
):
    """Get recent opportunities."""
    if not storage:
        return {"error": "Storage not initialized", "opportunities": []}

    opps = storage.get_recent_opportunities(hours=hours, limit=limit)

    if asset:
        opps = [o for o in opps if o["asset"].upper() == asset.upper()]

    return {
        "period_hours": hours,
        "count": len(opps),
        "opportunities": opps,
    }


@app.get("/api/opportunities/active")
async def get_active_opportunities():
    """Get currently active opportunities (combined < $1.00 right now)."""
    if not monitor:
        return {"error": "Monitor not initialized", "opportunities": []}

    spreads = monitor.get_current_spreads()
    active = [s for s in spreads if s.get("has_opportunity")]

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "count": len(active),
        "opportunities": active,
    }


# =============================================================================
# Statistics
# =============================================================================

@app.get("/api/stats")
async def get_stats(hours: int = Query(24, ge=1, le=168)):
    """Get opportunity statistics."""
    if not storage:
        return {"error": "Storage not initialized"}

    return storage.get_opportunity_stats(hours=hours)


@app.get("/api/stats/hourly")
async def get_hourly_stats(hours: int = Query(24, ge=1, le=168)):
    """Get hourly breakdown of opportunities."""
    if not storage:
        return {"error": "Storage not initialized", "hourly": []}

    return {
        "period_hours": hours,
        "hourly": storage.get_hourly_stats(hours=hours),
    }


@app.get("/api/stats/summary")
async def get_summary():
    """Get a quick summary for dashboard."""
    if not monitor or not storage:
        return {"error": "Not initialized"}

    status = monitor.get_status()
    stats_24h = storage.get_opportunity_stats(hours=24)
    stats_1h = storage.get_opportunity_stats(hours=1)
    current_spreads = monitor.get_current_spreads()

    # Find best current opportunity
    best_current = None
    if current_spreads:
        opps = [s for s in current_spreads if s.get("has_opportunity")]
        if opps:
            best_current = max(opps, key=lambda x: x.get("spread_pct") or 0)

    return {
        "monitor": {
            "connected": status.get("connected"),
            "markets_tracked": status.get("markets_tracked"),
            "active_opportunities": status.get("active_opportunities"),
        },
        "last_hour": {
            "opportunities": stats_1h.get("total_opportunities"),
            "best_spread_pct": stats_1h.get("max_spread_pct"),
        },
        "last_24h": {
            "opportunities": stats_24h.get("total_opportunities"),
            "avg_spread_pct": stats_24h.get("avg_spread_pct"),
            "best_spread_pct": stats_24h.get("max_spread_pct"),
            "total_opportunity_seconds": stats_24h.get("total_opportunity_seconds"),
            "by_asset": stats_24h.get("by_asset"),
        },
        "best_current": best_current,
    }


# =============================================================================
# Historical Data
# =============================================================================

@app.get("/api/snapshots")
async def get_snapshots(
    hours: int = Query(1, ge=1, le=24),
    limit: int = Query(100, ge=1, le=1000),
):
    """Get recent snapshots."""
    if not storage:
        return {"error": "Storage not initialized", "snapshots": []}

    return {
        "period_hours": hours,
        "snapshots": storage.get_recent_snapshots(hours=hours, limit=limit),
    }
