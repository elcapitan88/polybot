"""SQLAlchemy database models for Polymarket data collection."""

from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, Boolean, DateTime, Text,
    create_engine, Index
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class PriceTick(Base):
    """Individual price observation for a market."""
    __tablename__ = "price_ticks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    market_id = Column(String(100), nullable=False, index=True)
    asset = Column(String(10), nullable=False, index=True)
    question = Column(Text)

    yes_ask = Column(Float)
    no_ask = Column(Float)
    combined_cost = Column(Float)
    profit_pct = Column(Float)

    yes_liquidity = Column(Float)
    no_liquidity = Column(Float)

    has_arbitrage = Column(Boolean, default=False, index=True)
    is_cheap_yes = Column(Boolean, default=False)
    is_cheap_no = Column(Boolean, default=False)

    __table_args__ = (
        Index('ix_price_ticks_asset_timestamp', 'asset', 'timestamp'),
        Index('ix_price_ticks_market_timestamp', 'market_id', 'timestamp'),
    )


class MarketSession(Base):
    """Tracks a market session from open to close."""
    __tablename__ = "market_sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    market_id = Column(String(100), nullable=False, index=True)
    asset = Column(String(10), nullable=False, index=True)
    question = Column(Text)

    # Opening prices
    open_time = Column(DateTime, index=True)
    yes_open = Column(Float)
    no_open = Column(Float)

    # Closing prices
    close_time = Column(DateTime)
    yes_close = Column(Float)
    no_close = Column(Float)

    # Price extremes
    yes_high = Column(Float)
    yes_low = Column(Float)
    no_high = Column(Float)
    no_low = Column(Float)

    # Analysis results
    yes_closed_above_open = Column(Boolean)
    no_closed_below_open = Column(Boolean)
    yes_price_change = Column(Float)
    no_price_change = Column(Float)

    # Stats
    observation_count = Column(Integer, default=0)
    arbitrage_opportunities = Column(Integer, default=0)
    cheap_price_hits = Column(Integer, default=0)

    __table_args__ = (
        Index('ix_sessions_asset_open', 'asset', 'open_time'),
    )


class ArbitrageOpportunity(Base):
    """Recorded arbitrage opportunity."""
    __tablename__ = "arbitrage_opportunities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    market_id = Column(String(100), nullable=False, index=True)
    asset = Column(String(10), nullable=False, index=True)
    question = Column(Text)

    yes_ask = Column(Float)
    no_ask = Column(Float)
    combined_cost = Column(Float)
    profit_pct = Column(Float)

    yes_liquidity = Column(Float)
    no_liquidity = Column(Float)
    max_profit_usd = Column(Float)

    __table_args__ = (
        Index('ix_arb_asset_timestamp', 'asset', 'timestamp'),
        Index('ix_arb_profit', 'profit_pct'),
    )


class CheapPrice(Base):
    """Recorded cheap price occurrence."""
    __tablename__ = "cheap_prices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    market_id = Column(String(100), nullable=False, index=True)
    asset = Column(String(10), nullable=False, index=True)

    side = Column(String(5), nullable=False)  # YES or NO
    price = Column(Float, nullable=False, index=True)
    threshold = Column(Float)
    liquidity = Column(Float)

    __table_args__ = (
        Index('ix_cheap_asset_timestamp', 'asset', 'timestamp'),
        Index('ix_cheap_side_price', 'side', 'price'),
    )


class CollectorStats(Base):
    """Running stats for the collector."""
    __tablename__ = "collector_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime)

    total_scans = Column(Integer, default=0)
    total_opportunities = Column(Integer, default=0)
    total_cheap_prices = Column(Integer, default=0)
    total_sessions = Column(Integer, default=0)

    interval_ms = Column(Integer)
    cheap_threshold = Column(Float)
    arbitrage_threshold = Column(Float)


def init_db(database_url: str):
    """Initialize database connection and create tables."""
    engine = create_engine(database_url, pool_pre_ping=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return engine, Session
