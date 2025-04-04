from __future__ import annotations

from sqlalchemy import Column, String

from src.models.mixins import TimeAuditMixin
from src.database.connection import Base


class IqfeedSymbols(Base, TimeAuditMixin):
    __tablename__: str = "DTN_IQFEED_SYMBOLS_V_1"
    
    symbol = Column(String(255), primary_key=True)
    description = Column(String(255), nullable=False)
    security_type = Column(String(255), nullable=False)
    exchange = Column(String(255), nullable=False)
    listed_market = Column(String(255), nullable=False)