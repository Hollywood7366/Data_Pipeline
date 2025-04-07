from __future__ import annotations

from sqlalchemy import Column, String

from src.database.connection import Base
from src.models.mixins import TimeAuditMixin


class IqfeedSymbolsFrontMonth(Base, TimeAuditMixin):
    __tablename__: str = "DTN_IQFEED_SYMBOLS_FRONT_MONTH_FUTURES"

    symbol = Column(String(255), primary_key=True)
    description = Column(String(255), nullable=False)
    security_type = Column(String(255), nullable=False)
    exchange = Column(String(255), nullable=False)
    listed_market = Column(String(255), nullable=False)

class IqfeedSymbolsContinuousContracts(Base, TimeAuditMixin):
    __tablename__: str = "DTN_IQFEED_SYMBOLS_CONTINUOUS_CONTRACTS_FUTURES"

    symbol = Column(String(255), primary_key=True)
    description = Column(String(255), nullable=False)
    security_type = Column(String(255), nullable=False)
    exchange = Column(String(255), nullable=False)
    listed_market = Column(String(255), nullable=False)

class IqfeedSymbolsEminis(Base, TimeAuditMixin):
    __tablename__: str = "DTN_IQFEED_SYMBOLS_EMINIS"

    symbol = Column(String(255), primary_key=True)
    description = Column(String(255), nullable=False)
    security_type = Column(String(255), nullable=False)
    exchange = Column(String(255), nullable=False)
    listed_market = Column(String(255), nullable=False)

class IqfeedSymbolsNoOptions(Base, TimeAuditMixin):
    __tablename__: str = "DTN_IQFEED_SYMBOLS_NO_OPTIONS"

    symbol = Column(String(255), primary_key=True)
    description = Column(String(255), nullable=False)
    security_type = Column(String(255), nullable=False)
    exchange = Column(String(255), nullable=False)
    listed_market = Column(String(255), nullable=False)

class IqfeedSymbolsNoSpreads(Base, TimeAuditMixin):
    __tablename__: str = "DTN_IQFEED_SYMBOLS_NO_SPREADS"

    symbol = Column(String(255), primary_key=True)
    description = Column(String(255), nullable=False)
    security_type = Column(String(255), nullable=False)
    exchange = Column(String(255), nullable=False)
    listed_market = Column(String(255), nullable=False)

class IqfeedSymbolsAll(Base, TimeAuditMixin):
    __tablename__: str = "DTN_IQFEED_SYMBOLS_ALL"

    symbol = Column(String(255), primary_key=True)
    description = Column(String(255), nullable=False)
    security_type = Column(String(255), nullable=False)
    exchange = Column(String(255), nullable=False)
    listed_market = Column(String(255), nullable=False)
