"""PostgreSQL storage layer for spread monitoring data."""

from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from src.models.database import (
    init_db, SpreadSnapshot, Opportunity, MarketWindow, DailyStats
)


class Storage:
    """Storage interface for spread monitoring data."""

    def __init__(self, database_url: str):
        """Initialize database connection."""
        self.engine, self.SessionClass = init_db(database_url)

    def get_session(self) -> Session:
        """Get a new database session."""
        return self.SessionClass()

    def get_recent_opportunities(self, hours: int = 24, limit: int = 100) -> list[dict]:
        """Get recent opportunities."""
        session = self.get_session()
        try:
            since = datetime.utcnow() - timedelta(hours=hours)

            opps = session.query(Opportunity).filter(
                Opportunity.detected_at >= since
            ).order_by(desc(Opportunity.detected_at)).limit(limit).all()

            return [
                {
                    "id": o.id,
                    "detected_at": o.detected_at.isoformat() if o.detected_at else None,
                    "asset": o.asset,
                    "up_ask": o.up_ask,
                    "down_ask": o.down_ask,
                    "combined": o.combined,
                    "spread": o.spread,
                    "spread_pct": round(o.spread_pct, 2) if o.spread_pct else None,
                    "up_liquidity": o.up_liquidity,
                    "down_liquidity": o.down_liquidity,
                    "max_position": o.max_position,
                    "duration_seconds": o.duration_seconds,
                    "best_spread_pct": round(o.best_spread_pct, 2) if o.best_spread_pct else None,
                    "resolved": o.resolved_at is not None,
                }
                for o in opps
            ]
        finally:
            session.close()

    def get_opportunity_stats(self, hours: int = 24) -> dict:
        """Get opportunity statistics for the given period."""
        session = self.get_session()
        try:
            since = datetime.utcnow() - timedelta(hours=hours)

            total = session.query(func.count(Opportunity.id)).filter(
                Opportunity.detected_at >= since
            ).scalar() or 0

            avg_spread = session.query(func.avg(Opportunity.spread_pct)).filter(
                Opportunity.detected_at >= since
            ).scalar() or 0

            max_spread = session.query(func.max(Opportunity.spread_pct)).filter(
                Opportunity.detected_at >= since
            ).scalar() or 0

            avg_duration = session.query(func.avg(Opportunity.duration_seconds)).filter(
                Opportunity.detected_at >= since,
                Opportunity.duration_seconds.isnot(None)
            ).scalar() or 0

            total_duration = session.query(func.sum(Opportunity.duration_seconds)).filter(
                Opportunity.detected_at >= since,
                Opportunity.duration_seconds.isnot(None)
            ).scalar() or 0

            # By asset
            by_asset = {}
            for asset in ["BTC", "ETH", "SOL", "XRP"]:
                count = session.query(func.count(Opportunity.id)).filter(
                    Opportunity.detected_at >= since,
                    Opportunity.asset == asset
                ).scalar() or 0
                by_asset[asset] = count

            return {
                "period_hours": hours,
                "total_opportunities": total,
                "avg_spread_pct": round(avg_spread, 2),
                "max_spread_pct": round(max_spread, 2),
                "avg_duration_seconds": round(avg_duration, 1),
                "total_opportunity_seconds": round(total_duration, 1),
                "by_asset": by_asset,
            }
        finally:
            session.close()

    def get_recent_snapshots(self, hours: int = 1, limit: int = 500) -> list[dict]:
        """Get recent snapshots."""
        session = self.get_session()
        try:
            since = datetime.utcnow() - timedelta(hours=hours)

            snaps = session.query(SpreadSnapshot).filter(
                SpreadSnapshot.timestamp >= since
            ).order_by(desc(SpreadSnapshot.timestamp)).limit(limit).all()

            return [
                {
                    "timestamp": s.timestamp.isoformat() if s.timestamp else None,
                    "asset": s.asset,
                    "up_ask": s.up_ask,
                    "down_ask": s.down_ask,
                    "combined": s.combined,
                    "spread": s.spread,
                    "has_opportunity": s.has_opportunity,
                }
                for s in snaps
            ]
        finally:
            session.close()

    def get_hourly_stats(self, hours: int = 24) -> list[dict]:
        """Get hourly aggregated stats."""
        session = self.get_session()
        try:
            since = datetime.utcnow() - timedelta(hours=hours)

            # Group by hour
            results = session.query(
                func.date_trunc('hour', Opportunity.detected_at).label('hour'),
                func.count(Opportunity.id).label('count'),
                func.avg(Opportunity.spread_pct).label('avg_spread'),
                func.max(Opportunity.spread_pct).label('max_spread'),
            ).filter(
                Opportunity.detected_at >= since
            ).group_by(
                func.date_trunc('hour', Opportunity.detected_at)
            ).order_by('hour').all()

            return [
                {
                    "hour": r.hour.isoformat() if r.hour else None,
                    "count": r.count,
                    "avg_spread_pct": round(r.avg_spread, 2) if r.avg_spread else 0,
                    "max_spread_pct": round(r.max_spread, 2) if r.max_spread else 0,
                }
                for r in results
            ]
        finally:
            session.close()
