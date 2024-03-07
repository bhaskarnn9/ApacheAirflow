from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

default_args = {'owner': 'bhas.neel'}


def print_function():
    print("The simplest possible python operator!")


with DAG(
        dag_id='execute_single_python_operators',
        description='DAG using python operator',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@daily',
        tags=['simple', 'python']
) as dad:
    task = PythonOperator(
        task_id='python_task',
        python_callable=print_function
    )
