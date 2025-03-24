from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from src.database.connection import QuestDBConnection
from utils.logging import Logger

logger = Logger(name='iqfeed', log_dir='data/logs')

class QuestDBOperations(QuestDBConnection):
    def __init__(
        self,
        table_name: str,
        schema: Dict[str, str] = None,
        create_if_not_exists: bool = True,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.table_name = table_name
        
        self.schema = schema
        self.create_if_not_exists = create_if_not_exists

    async def initialize(self) -> None:
        await self.connect()
        if self.create_if_not_exists:
            await self.create_table_if_not_exists()

    async def cleanup(self) -> None:
        await self.disconnect()
        
    async def insert_data(self, data: Dict[str, Any]) -> None:
        """Insert a single record into the database with proper string handling"""
        try:
            logger.info(f"Inserting record into {self.table_name}: {data}")
            
            sanitized_data = {}
            for key, value in data.items():
                if isinstance(value, str):
                    sanitized_value = value.replace("'", "''")
                    sanitized_data[key] = sanitized_value
                else:
                    sanitized_data[key] = value
            
            columns = ', '.join(sanitized_data.keys())
            values = []
            placeholders = []
            
            for i, val in enumerate(sanitized_data.values()):
                values.append(val)
                if isinstance(val, str):
                    placeholders.append(f"$::{i+1}::string")
                else:
                    placeholders.append(f"${i+1}")
            
            placeholders_str = ', '.join(placeholders)
            
            query = f"""
                INSERT INTO {self.table_name} ({columns})
                VALUES ({placeholders_str})
            """
            
            await self.execute_query(query, tuple(values))
            logger.info(f"Successfully inserted record into {self.table_name}")
        except Exception as e:
            logger.error(f"Error inserting data into {self.table_name}: {e}")
            raise


    async def create_table_if_not_exists(self) -> None:
        if not await self.table_exists(self.table_name):
            columns = [f"{col_name} {col_type}" for col_name, col_type in self.schema.items()]
            columns_str = ", ".join(columns)
            
            query = f"""
                CREATE TABLE {self.table_name} (
                    {columns_str},
                    created_at TIMESTAMP
                ) TIMESTAMP(created_at) PARTITION BY DAY;
            """
            
            try:
                await self.execute_query(query)
                logger.info(f"Table '{self.table_name}' created successfully.")
            except Exception as e:
                if "already exists" in str(e).lower():
                    logger.info(f"Table '{self.table_name}' already exists. Skipping creation.")
                else:
                    raise Exception(f"Failed to create table: {str(e)}")

    async def get_all(self) -> List[Dict]:
        query = f"SELECT * FROM {self.table_name}"
        return await self.execute_query(query)

    async def get_by_column(self, column: str, value: Any) -> List[Dict]:
        query = f"SELECT * FROM {self.table_name} WHERE {column} = $1"
        return await self.execute_query(query, (value,))

    async def get_one(self, id_column: str, id_value: Any) -> Optional[Dict]:
        query = f"SELECT * FROM {self.table_name} WHERE {id_column} = $1 LIMIT 1"
        result = await self.execute_query(query, (id_value,))
        return result[0] if result else None

    async def create(self, data: Dict[str, Any]) -> None:
        if 'created_at' in self.schema and 'created_at' not in data:
            data['created_at'] = datetime.now()
        if 'updated_at' in self.schema and 'updated_at' not in data:
            data['updated_at'] = datetime.now()

        columns = list(data.keys())
        placeholders = [f"${i+1}" for i in range(len(data))]
        query = f"""
            INSERT INTO {self.table_name} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """
        await self.execute_query(query, tuple(data.values()))

    async def update(self, id_column: str, id_value: Any, data: Dict[str, Any]) -> None:
        if 'updated_at' in self.schema and 'updated_at' not in data:
            data['updated_at'] = datetime.now()

        set_items = [f"{k} = ${i+1}" for i, k in enumerate(data.keys())]
        query = f"""
            UPDATE {self.table_name}
            SET {', '.join(set_items)}
            WHERE {id_column} = ${len(data) + 1}
        """
        values = tuple(data.values()) + (id_value,)
        await self.execute_query(query, values)

    async def delete(self, id_column: str, id_value: Any) -> None:
        query = f"DELETE FROM {self.table_name} WHERE {id_column} = $1"
        await self.execute_query(query, (id_value,))

    async def sort(self, column: str, ascending: bool = True) -> List[Dict]:
        direction = "ASC" if ascending else "DESC"
        query = f"SELECT * FROM {self.table_name} ORDER BY {column} {direction}"
        return await self.execute_query(query)

    async def get_with_sort(
        self,
        filter_column: str,
        filter_value: Any,
        sort_column: str,
        ascending: bool = True
    ) -> List[Dict]:
        direction = "ASC" if ascending else "DESC"
        query = f"""
            SELECT * FROM {self.table_name}
            WHERE {filter_column} = $1
            ORDER BY {sort_column} {direction}
        """
        return await self.execute_query(query, (filter_value,))

    async def add_column_toexisting(
        self,
        column_name: str,
        column_type: str,
        default_value: Any = None
    ) -> None:
        default_clause = f" DEFAULT {default_value}" if default_value is not None else ""
        query = f"ALTER TABLE {self.table_name} ADD COLUMN {column_name} {column_type}{default_clause}"
        await self.execute_query(query)
        self.schema[column_name] = column_type

    async def get_columns(self) -> List[Dict]:
        query = f"SHOW COLUMNS FROM {self.table_name}"
        return await self.execute_query(query)

    async def get_count(self) -> int:
        query = f"SELECT COUNT(*) as count FROM {self.table_name}"
        result = await self.execute_query(query)
        return result[0]['count']

    async def get_with_pagination(
        self,
        page: int = 1,
        page_size: int = 10,
        sort_column: str = None
    ) -> Tuple[List[Dict], int]:
        offset = (page - 1) * page_size
        sort_clause = f"ORDER BY {sort_column}" if sort_column else ""
        query = f"""
            SELECT * FROM {self.table_name}
            {sort_clause}
            LIMIT $1 OFFSET $2
        """
        results = await self.execute_query(query, (page_size, offset))
        total_count = await self.get_count()
        return results, total_count

    async def bulk_create(self, data_list: List[Dict[str, Any]]) -> None:
        if not data_list:
            return
        
        current_time = datetime.now()
        for data in data_list:
            if 'created_at' in self.schema and 'created_at' not in data:
                data['created_at'] = current_time
            if 'updated_at' in self.schema and 'updated_at' not in data:
                data['updated_at'] = current_time

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                columns = list(data_list[0].keys())
                for data in data_list:
                    placeholders = [f"${i+1}" for i in range(len(data))]
                    query = f"""
                        INSERT INTO {self.table_name} ({', '.join(columns)})
                        VALUES ({', '.join(placeholders)})
                    """
                    await conn.execute(query, *tuple(data.values()))
                    
    async def drop_table(self, if_exists: bool = True) -> None:
        await self.ensure_connected()
        try:
            if_exists_clause = "IF EXISTS" if if_exists else ""
            query = f"DROP TABLE {if_exists_clause} {self.table_name}"
            await self.execute_query(query)
        except Exception as e:
            raise Exception(f"Failed to drop table: {str(e)}")

    async def truncate_table(self) -> None:
        await self.ensure_connected()
        try:
            print('TruncATE: ', self.table_name)
            query = f"TRUNCATE TABLE {self.table_name}"
            await self.execute_query(query)
        except Exception as e:
            raise Exception(f"Failed to truncate table: {str(e)}")