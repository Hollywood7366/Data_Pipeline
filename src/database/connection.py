from typing import Any
import asyncpg

class QuestDBConnection:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8812,
        username: str = "admin",
        password: str = "quest",
        database: str = "qdb"
    ):
        self.connection_params = {
            "host": host,
            "port": port,
            "user": username,
            "password": password,
            "database": database
        }
        self.pool = None
        self._initialized = False

    async def connect(self) -> None:
        if not self._initialized:
            try:
                self.pool = await asyncpg.create_pool(**self.connection_params)
                self._initialized = True
            except Exception as e:
                raise Exception(f"Failed to connect to QuestDB: {str(e)}")

    async def disconnect(self) -> None:
        if self.pool:
            await self.pool.close()
            self._initialized = False

    async def ensure_connected(self) -> None:
        if not self._initialized:
            await self.connect()

    async def execute_query(self, query: str, params: tuple = None) -> Any:
        await self.ensure_connected()
        
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    if query.strip().upper().startswith(('SELECT', 'SHOW')):
                        results = await conn.fetch(query, *params if params else ())
                        return [dict(row) for row in results]
                    else:
                        await conn.execute(query, *params if params else ())
                        return None
                except Exception as e:
                    raise Exception(f"Query execution failed: {str(e)}")

    async def table_exists(self, table_name: str) -> bool:
        try:
            query = """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = $1
                )
            """
            result = await self.execute_query(query, (table_name,))
            return result[0]['exists']
        except Exception:
            return False