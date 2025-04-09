from __future__ import annotations

from contextvars import ContextVar, Token

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from src.config.config import config as cn

Base = declarative_base()
session_context: ContextVar[str] = ContextVar("session_context")


def get_session_context() -> str:
    return session_context.get()


def set_session_context(session_id: str) -> Token:
    return session_context.set(session_id)


def reset_session_context(context: Token) -> None:
    session_context.reset(context)


local_engine = create_async_engine(
    url=cn.LOCAL_DATABASE_URL,
    pool_recycle=3600,
)

engine = create_async_engine(
    url=cn.DATABASE_URL,
    pool_recycle=3600,
)

async_local_session = sessionmaker(
    local_engine,
    expire_on_commit=False,
    class_=AsyncSession,
)

async_session = sessionmaker(
    engine,
    expire_on_commit=False,
    class_=AsyncSession,
)
