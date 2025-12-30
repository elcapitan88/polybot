"""PostgreSQL storage layer for Polymarket data collection."""

from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session

from src.models.database import (
    init_db,
    PriceTick,
    MarketSession as DBMarketSession,
    ArbitrageOpportunity,
    CheapPrice,
    CollectorStats
)


class PostgresStorage:
    """PostgreSQL storage for price data and analysis."""

    def __init__(self, database_url: str):
        """Initialize database connection."""
        self.engine, self.SessionClass = init_db(database_url)
        self._session: Optional[Session] = None

    def get_session(self) -> Session:
        """Get or create database session."""
        if self._session is None:
            self._session = self.SessionClass()
        return self._session

    def commit(self):
        """Commit pending changes."""
        if self._session:
            self._session.commit()

    def close(self):
        """Close database session."""
        if self._session:
            self._session.close()
            self._session = None

    def log_tick(
        self,
        timestamp: datetime,
        market_id: str,
        asset: str,
        question: str,
        yes_ask: Optional[float],
        no_ask: Optional[float],
        combined_cost: Optional[float],
        profit_pct: Optional[float],
        yes_liquidity: float,
        no_liquidity: float,
        has_arbitrage: bool,
        is_cheap_yes: bool,
        is_cheap_no: bool
    ):
        """Log a single price tick."""
        session = self.get_session()
        tick = PriceTick(
            timestamp=timestamp,
            market_id=market_id,
            asset=asset,
            question=question[:500] if question else None,
            yes_ask=yes_ask,
            no_ask=no_ask,
            combined_cost=combined_cost,
            profit_pct=profit_pct,
            yes_liquidity=yes_liquidity,
            no_liquidity=no_liquidity,
            has_arbitrage=has_arbitrage,
            is_cheap_yes=is_cheap_yes,
            is_cheap_no=is_cheap_no
        )
        session.add(tick)

    def log_opportunity(
        self,
        timestamp: datetime,
        market_id: str,
        asset: str,
        question: str,
        yes_ask: float,
        no_ask: float,
        combined_cost: float,
        profit_pct: float,
        yes_liquidity: float,
        no_liquidity: float,
        max_profit_usd: float
    ):
        """Log an arbitrage opportunity."""
        session = self.get_session()
        opp = ArbitrageOpportunity(
            timestamp=timestamp,
            market_id=market_id,
            asset=asset,
            question=question[:500] if question else None,
            yes_ask=yes_ask,
            no_ask=no_ask,
            combined_cost=combined_cost,
            profit_pct=profit_pct,
            yes_liquidity=yes_liquidity,
            no_liquidity=no_liquidity,
            max_profit_usd=max_profit_usd
        )
        session.add(opp)

    def log_cheap_price(
        self,
        timestamp: datetime,
        market_id: str,
        asset: str,
        side: str,
        price: float,
        threshold: float,
        liquidity: float
    ):
        """Log a cheap price occurrence."""
        session = self.get_session()
        cheap = CheapPrice(
            timestamp=timestamp,
            market_id=market_id,
            asset=asset,
            side=side,
            price=price,
            threshold=threshold,
            liquidity=liquidity
        )
        session.add(cheap)

    def save_session(
        self,
        market_id: str,
        asset: str,
        question: str,
        open_time: Optional[datetime],
        close_time: Optional[datetime],
        yes_open: Optional[float],
        yes_close: Optional[float],
        yes_high: Optional[float],
        yes_low: Optional[float],
        no_open: Optional[float],
        no_close: Optional[float],
        no_high: Optional[float],
        no_low: Optional[float],
        yes_closed_above_open: Optional[bool],
        no_closed_below_open: Optional[bool],
        yes_price_change: Optional[float],
        no_price_change: Optional[float],
        observation_count: int,
        arbitrage_opportunities: int,
        cheap_price_hits: int
    ):
        """Save a completed market session."""
        session = self.get_session()
        market_session = DBMarketSession(
            market_id=market_id,
            asset=asset,
            question=question[:500] if question else None,
            open_time=open_time,
            close_time=close_time,
            yes_open=yes_open,
            yes_close=yes_close,
            yes_high=yes_high,
            yes_low=yes_low,
            no_open=no_open,
            no_close=no_close,
            no_high=no_high,
            no_low=no_low,
            yes_closed_above_open=yes_closed_above_open,
            no_closed_below_open=no_closed_below_open,
            yes_price_change=yes_price_change,
            no_price_change=no_price_change,
            observation_count=observation_count,
            arbitrage_opportunities=arbitrage_opportunities,
            cheap_price_hits=cheap_price_hits
        )
        session.add(market_session)

    def start_collector_run(
        self,
        interval_ms: int,
        cheap_threshold: float,
        arbitrage_threshold: float
    ) -> int:
        """Start a new collector run, returns run ID."""
        session = self.get_session()
        stats = CollectorStats(
            start_time=datetime.utcnow(),
            interval_ms=interval_ms,
            cheap_threshold=cheap_threshold,
            arbitrage_threshold=arbitrage_threshold
        )
        session.add(stats)
        session.commit()
        return stats.id

    def update_collector_stats(
        self,
        run_id: int,
        total_scans: int,
        total_opportunities: int,
        total_cheap_prices: int,
        total_sessions: int
    ):
        """Update collector run stats."""
        session = self.get_session()
        stats = session.query(CollectorStats).get(run_id)
        if stats:
            stats.total_scans = total_scans
            stats.total_opportunities = total_opportunities
            stats.total_cheap_prices = total_cheap_prices
            stats.total_sessions = total_sessions
            stats.end_time = datetime.utcnow()

    def get_stats_summary(self) -> dict:
        """Get overall stats summary."""
        session = self.get_session()

        total_ticks = session.query(PriceTick).count()
        total_opportunities = session.query(ArbitrageOpportunity).count()
        total_cheap = session.query(CheapPrice).count()
        total_sessions = session.query(DBMarketSession).count()

        # Pattern analysis
        sessions_with_data = session.query(DBMarketSession).filter(
            DBMarketSession.yes_closed_above_open.isnot(None)
        ).all()

        yes_up_count = sum(1 for s in sessions_with_data if s.yes_closed_above_open)
        no_down_count = sum(1 for s in sessions_with_data if s.no_closed_below_open)

        return {
            "total_ticks": total_ticks,
            "total_opportunities": total_opportunities,
            "total_cheap_prices": total_cheap,
            "total_sessions": total_sessions,
            "sessions_with_pattern_data": len(sessions_with_data),
            "yes_closed_above_open_count": yes_up_count,
            "no_closed_below_open_count": no_down_count,
            "yes_up_pct": (yes_up_count / len(sessions_with_data) * 100) if sessions_with_data else 0,
            "no_down_pct": (no_down_count / len(sessions_with_data) * 100) if sessions_with_data else 0,
        }
