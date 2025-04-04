from __future__ import annotations

from contextvars import ContextVar, Token

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from src.config.config import config
import logging

Base = declarative_base()
session_context: ContextVar[str] = ContextVar("session_context")


def get_session_context() -> str:
    return session_context.get()


def set_session_context(session_id: str) -> Token:
    return session_context.set(session_id)


def reset_session_context(context: Token) -> None:
    session_context.reset(context)

logging.info(f"LOCAL_DATABASE_URL: {config.LOCAL_DATABASE_URL}")
logging.info(f"DATABASE_URL: {config.DATABASE_URL}")

local_engine = create_async_engine(
    url="mysql+aiomysql://root:1234@localhost:3306/probabilitiesunlimited",
    pool_recycle=3600,
)

engine = create_async_engine(
    url="mysql+aiomysql://root:1234@host.docker.internal:3306/probabilitiesunlimited",
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