from __future__ import annotations

from sqlalchemy import TIMESTAMP, Column, Float, Integer, String

from src.database.connection import Base
from src.models.mixins import TimeAuditMixin


class SymbolMetadata(Base, TimeAuditMixin):
    __tablename__ = "DTN_SYMBOLS_META"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(255), nullable=False)
    count = Column(Integer, nullable=False, default=0)
    status = Column(String(255), nullable=True)


class IQFeedDataMeta(Base, TimeAuditMixin):
    __tablename__ = "IQFEED_READY_META_DATA"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(255), nullable=False)
    count = Column(Integer, nullable=False, default=0)
    last_record_datetime = Column(TIMESTAMP(timezone=True), nullable=False)
    status = Column(String(255), nullable=True)
    max_high = Column(Float, nullable=False)
    min_low = Column(Float, nullable=False)
    total_volume = Column(Integer, nullable=False)
    average_close = Column(Float, nullable=False)
    last_processed_period = Column(String(255), nullable=False)

    def __init__(
        self,
        symbol: str,
        count: int = 0,
        last_record_datetime=None,
        status: str = "added",
        max_high: float = 0.0,
        min_low: float = 0.0,
        total_volume: int = 0,
        average_close: float = 0.0,
        last_processed_period: str = "daily",
    ):
        self.symbol = symbol
        self.count = count
        self.last_record_datetime = last_record_datetime
        self.status = status
        self.max_high = max_high
        self.min_low = min_low
        self.total_volume = total_volume
        self.average_close = average_close
        self.last_processed_period = last_processed_period

    def __repr__(self):
        return f"<SymbolMetadata(symbol={self.symbol}, count={self.count}, status={self.status})>"


class History(Base, TimeAuditMixin):
    __tablename__ = "RUN_HISTORY"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_date = Column(TIMESTAMP(timezone=True), nullable=False)
    symbol_count = Column(Integer, nullable=False)
    run_count = Column(Integer, default=1)

    def __init__(self, run_date, symbol_count, run_count=1):
        self.run_date = run_date
        self.symbol_count = symbol_count
        self.run_count = run_count

    def __repr__(self):
        return f"<ScriptRunMetadata(run_date={self.run_date}, symbol_count={self.symbol_count}, run_count={self.run_count})>"
