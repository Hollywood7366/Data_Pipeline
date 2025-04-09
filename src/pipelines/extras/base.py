from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.future import Select
from sqlalchemy.sql.expression import select

from src.database.connection import (
    AsyncSession,
    engine,
    local_engine,
    sessionmaker,
)
from utils.logging import Logger

logger = Logger(name="iqfeed", log_dir="data/logs")


class BaseDB:
    def __init__(self, model, local=False) -> None:
        self.model = model
        if local:
            self.async_session = sessionmaker(
                local_engine,
                expire_on_commit=False,
                class_=AsyncSession,
            )
        else:
            self.async_session = sessionmaker(
                engine,
                expire_on_commit=False,
                class_=AsyncSession,
            )

    async def bulk_insert(self, data) -> None:
        async with self.async_session() as session:
            try:
                await session.run_sync(
                    lambda sessio: sessio.bulk_insert_mappings(
                        self.model, data
                    ),
                )
                await session.commit()
                logger.info(
                    message=f"Successfully added {len(data)} records to the {self.model.__name__} table",
                )
            except Exception as e:
                await session.rollback()
                logger.critical(
                    message=f"Failed to load data into {self.model.__name__} \n Error: {e}",
                )

    async def create(self, data) -> None:
        async with self.async_session() as session:
            try:
                session.add(self.model(**data))
                await session.commit()
                logger.info(
                    f"Successfully added a record to the {self.model.__name__} table"
                )
            except Exception as e:
                await session.rollback()
                logger.critical(
                    message=f"Failed to insert data into {self.model.__name__} \n Error: {e}",
                )

    async def update(self, instance) -> None:
        async with self.async_session() as session:
            try:
                session.add(instance)
                await session.commit()
                logger.info(
                    f"Successfully updated a record in the {self.model.__name__} table"
                )
            except Exception as e:
                await session.rollback()
                logger.critical(
                    message=f"Failed to update data in {self.model.__name__} \n Error: {e}",
                )

    async def _get_all(self):
        query = select(self.model)
        return await self._all(query)

    async def _all(self, query: Select):
        async with self.async_session() as session:
            query = await session.execute(query)
            return query.unique().scalars().all()

    async def _query(
        self,
    ) -> Select:
        query = select(self.model)
        return query

    async def _one_or_none(self, query: Select):
        async with self.async_session() as session:
            query = await session.scalars(query)
            query = query.unique()
            return query.one_or_none()

    async def get_by_column(
        self,
        column: str,
        value: Any,
        unique: bool = False,
    ):
        try:
            query = select(self.model)
            query = await self._get_by(self.model, query, column, value)

            if unique:
                return await self._one_or_none(query)

            return await self._all(query)
        except Exception as e:
            logger.critical(
                message=f"Failed to retrieve data from {self.model.__name__} \n Error: {e}",
            )
            return []

    async def get_all_by_column(
        self,
        column: str,
    ):
        try:
            query = select(getattr(self.model, column))
            return await self._all(query)
        except Exception as e:
            logger.critical(
                message=f"Failed to retrieve data from {self.model.__name__} \n Error: {e}",
            )
            return []

    async def _get_by(
        self,
        model_class,
        query: Select,
        field: str,
        value: Any,
    ) -> Select:
        return query.where(getattr(model_class, field) == value)

    async def truncate_table(self):
        async with self.async_session() as session:
            try:
                async with session.begin():
                    # await session.execute(f"USE {database}")
                    await session.execute(
                        f"TRUNCATE TABLE {self.model.__table__}"
                    )
                logger.info(
                    message=f"Successfully truncated the {self.model.__tablename__} table",
                )
            except Exception as e:
                logger.critical(
                    message=f"Failed to truncate {self.model.__tablename__} \n Error: {e}",
                )
                return []
        return

    async def get_unique_column(self, column: str):
        try:
            query = select(getattr(self.model, column)).distinct()
            return await self._all(query)
        except Exception as e:
            logger.critical(
                message=f"Failed to retrieve data from {self.model.__name__} \n Error: {e}",
            )
            return []

    async def _execute(self, queries: list[str] | str):
        async with self.async_session() as session:
            if isinstance(queries, list):
                for query in queries:
                    await session.execute(query)
            else:
                await session.execute(queries)
            await session.commit()
            await session.close()
