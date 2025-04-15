from sqlalchemy import Column, Date, Integer, String, DateTime, Boolean
from src.database.connection import Base

class TickerExtraction(Base):
    __tablename__ = 'ticker_progress'
    
    id = Column(Integer, primary_key=True)
    ticker = Column(String(20), nullable=False, index=True)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)
    interval = Column(String(20), nullable=False)
    extraction_date = Column(Date, nullable=False)
    successful = Column(Boolean, default=False)
    record_count = Column(Integer, default=0)
    
    def __repr__(self):
        return f"<TickerExtraction(ticker='{self.ticker}', start='{self.start_date}', end='{self.end_date}', successful={self.successful})>"