import sys, os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import asyncio
from typing import Dict, Any
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append("/opt/airflow")

from src.pipelines.extras.base import QuestDBOperations

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 8812,
    'username': 'admin',
    'password': 'quest',
}

# Schema definition
SCHEMA = {
    "id": "LONG",
    "name": "STRING",
    "email": "STRING",
    "created_at": "TIMESTAMP",
    "updated_at": "TIMESTAMP"
}

# Sample test data
TEST_DATA = {
    "id": 1,
    "name": "John Doe",
    "email": "john@example.com"
}

async def init_database():
    """Initialize database and create table"""
    db = QuestDBOperations(
        table_name="test_users",
        schema=SCHEMA,
        create_if_not_exists=True,
        **DB_CONFIG
    )
    await db.initialize()
    return db

def run_async(coroutine):
    """Helper function to run async code in sync context"""
    return asyncio.get_event_loop().run_until_complete(coroutine)

async def create_record(db: QuestDBOperations, data: Dict[str, Any]):
    """Create a test record"""
    await db.create(data)
    return "Record created successfully"

async def get_records(db: QuestDBOperations):
    """Get all records"""
    records = await db.get_all()
    return json.dumps(records, default=str)

async def update_record(db: QuestDBOperations, id_value: int, data: Dict[str, Any]):
    """Update a test record"""
    await db.update("id", id_value, data)
    return "Record updated successfully"

async def delete_record(db: QuestDBOperations, id_value: int):
    """Delete a test record"""
    await db.delete("id", id_value)
    return "Record deleted successfully"

# Task functions that wrap async operations
def task_create_record(**context):
    db = run_async(init_database())
    result = run_async(create_record(db, TEST_DATA))
    run_async(db.cleanup())
    return result

def task_get_records(**context):
    db = run_async(init_database())
    result = run_async(get_records(db))
    run_async(db.cleanup())
    return result

def task_update_record(**context):
    db = run_async(init_database())
    updated_data = {
        "name": "John Updated",
        "email": "john.updated@example.com"
    }
    result = run_async(update_record(db, TEST_DATA["id"], updated_data))
    run_async(db.cleanup())
    return result

def task_delete_record(**context):
    db = run_async(init_database())
    result = run_async(delete_record(db, TEST_DATA["id"]))
    run_async(db.cleanup())
    return result

with DAG(
    'questdb_crud_test',
    default_args=default_args,
    description='Test QuestDB CRUD operations',
    schedule_interval=None,
    start_date=datetime(2024, 2, 23),
    catchup=False,
    tags=['questdb', 'test'],
) as dag:

    # Create tasks
    create_task = PythonOperator(
        task_id='create_record',
        python_callable=task_create_record,
    )

    get_task_1 = PythonOperator(
        task_id='get_records_1',
        python_callable=task_get_records,
    )

    update_task = PythonOperator(
        task_id='update_record',
        python_callable=task_update_record,
    )

    get_task_2 = PythonOperator(
        task_id='get_records_2',
        python_callable=task_get_records,
    )

    delete_task = PythonOperator(
        task_id='delete_record',
        python_callable=task_delete_record,
    )

    get_task_3 = PythonOperator(
        task_id='get_records_3',
        python_callable=task_get_records,
    )

    # Set task dependencies
    create_task >> get_task_1 >> update_task >> get_task_2 >> delete_task >> get_task_3