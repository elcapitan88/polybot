"""SQLAlchemy database models for Polymarket spread monitoring.

Designed for the Jane Street delta-neutral strategy:
- Track when combined UP + DOWN < $1.00 (opportunity)
- Monitor spread frequency, duration, and size
- Store historical data for pattern analysis
"""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, Boolean, DateTime,
    create_engine, Index, BigInteger
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class SpreadSnapshot(Base):
    """
    Periodic snapshot of market spreads.
    Stored every 30 seconds for historical analysis.
    """
    __tablename__ = "spread_snapshots"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True)

    # Market identification
    asset = Column(String(10), nullable=False, index=True)  # BTC, ETH, SOL, XRP
    market_id = Column(String(100), nullable=False)

    # Prices (what you pay to BUY each side)
    up_ask = Column(Float)      # Price to buy UP (YES)
    down_ask = Column(Float)    # Price to buy DOWN (NO)
    combined = Column(Float)    # up_ask + down_ask
    spread = Column(Float)      # 1.0 - combined (positive = profit opportunity)

    # Liquidity available at best ask
    up_liquidity = Column(Float)
    down_liquidity = Column(Float)

    # Flags
    has_opportunity = Column(Boolean, default=False, index=True)  # combined < 1.0

    __table_args__ = (
        Index('ix_snapshots_asset_ts', 'asset', 'timestamp'),
        Index('ix_snapshots_opportunity', 'has_opportunity', 'timestamp'),
    )


class Opportunity(Base):
    """
    Recorded when combined < $1.00 - these are the money moments.
    Tracks full lifecycle: detection -> resolution.
    """
    __tablename__ = "opportunities"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # When detected
    detected_at = Column(DateTime, nullable=False, index=True)

    # Market info
    asset = Column(String(10), nullable=False, index=True)
    market_id = Column(String(100), nullable=False)

    # Prices at detection
    up_ask = Column(Float, nullable=False)
    down_ask = Column(Float, nullable=False)
    combined = Column(Float, nullable=False)
    spread = Column(Float, nullable=False)  # 1.0 - combined
    spread_pct = Column(Float, nullable=False)  # (spread / combined) * 100

    # Liquidity at detection
    up_liquidity = Column(Float)
    down_liquidity = Column(Float)
    max_position = Column(Float)  # min(up_liq, down_liq) - max you could trade

    # Lifecycle tracking
    resolved_at = Column(DateTime)  # When spread closed (combined >= 1.0)
    duration_seconds = Column(Float)  # How long opportunity lasted

    # Best prices during opportunity
    best_spread = Column(Float)  # Maximum spread seen
    best_spread_pct = Column(Float)

    __table_args__ = (
        Index('ix_opp_asset_detected', 'asset', 'detected_at'),
        Index('ix_opp_spread', 'spread_pct'),
    )


class MarketWindow(Base):
    """
    Summary stats for each 5m/15m market window.
    Useful for understanding when opportunities occur.
    """
    __tablename__ = "market_windows"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    # Window identification
    asset = Column(String(10), nullable=False, index=True)
    timeframe = Column(String(5), nullable=False)  # "5m" or "15m"
    market_id = Column(String(100), nullable=False)

    # Time bounds
    start_time = Column(DateTime, nullable=False, index=True)
    end_time = Column(DateTime)

    # Spread stats during window
    min_combined = Column(Float)  # Lowest combined cost seen
    max_spread = Column(Float)    # Best spread seen
    avg_combined = Column(Float)  # Average combined cost

    # Opportunity stats
    opportunity_count = Column(Integer, default=0)  # Times combined < $1.00
    total_opportunity_seconds = Column(Float, default=0)  # Total time with opportunity

    # Snapshot count
    snapshot_count = Column(Integer, default=0)

    __table_args__ = (
        Index('ix_window_asset_start', 'asset', 'start_time'),
    )


class DailyStats(Base):
    """
    Daily aggregated statistics for reporting.
    """
    __tablename__ = "daily_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, nullable=False, unique=True, index=True)

    # Opportunity counts
    total_opportunities = Column(Integer, default=0)
    btc_opportunities = Column(Integer, default=0)
    eth_opportunities = Column(Integer, default=0)
    sol_opportunities = Column(Integer, default=0)
    xrp_opportunities = Column(Integer, default=0)

    # Best spreads seen
    best_spread_pct = Column(Float)
    avg_spread_pct = Column(Float)

    # Time stats
    total_opportunity_seconds = Column(Float, default=0)
    longest_opportunity_seconds = Column(Float)

    # If we had traded (theoretical)
    theoretical_profit_usd = Column(Float)  # Based on $100 per opportunity


class MonitorStatus(Base):
    """
    Current monitor status and health.
    """
    __tablename__ = "monitor_status"

    id = Column(Integer, primary_key=True, autoincrement=True)
    started_at = Column(DateTime, nullable=False)
    last_update = Column(DateTime)

    # Connection status
    websocket_connected = Column(Boolean, default=False)
    subscribed_markets = Column(Integer, default=0)

    # Stats since start
    snapshots_recorded = Column(Integer, default=0)
    opportunities_detected = Column(Integer, default=0)

    # Current state
    active_opportunities = Column(Integer, default=0)


def init_db(database_url: str):
    """Initialize database connection and create tables."""
    engine = create_engine(database_url, pool_pre_ping=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return engine, Session
