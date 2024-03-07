from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.utils.dates import days_ago

default_args = {'owner': 'bhas.neel'}


with DAG(
        dag_id='create_sql_table',
        description='create sql table to test connection',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['python', 'sql', 'connection']
) as dag:
    create_table = SqliteOperator(
        task_id='create_table',
        sql=r'''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                age INTEGER NOT NULL,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''',
        sqlite_conn_id='bneel_sqlite_connection'
    )
