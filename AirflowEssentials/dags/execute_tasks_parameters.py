import time
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

default_args = {'owner': 'bhas.neel'}


def greet_hello(name):
    print('Hello, {name}!!'.format(name=name))


def greet_hello_with_city(name, city):
    print('Hello, {name} from {city}!!'.format(name=name, city=city))


with DAG(
        dag_id='execute_tasks_parameters',
        description='Python operators in DAGs with parameters',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@daily',
        tags=['parameters', 'python']
) as dag:
    taskA = PythonOperator(
        task_id='greet_hello',
        python_callable=greet_hello,
        op_kwargs={'name': 'Desmond'}
    )

    taskB = PythonOperator(
        task_id='greet_hello_with_city',
        python_callable=greet_hello_with_city,
        op_kwargs={'name': 'Raymond', 'city': 'Austin'}
    )

taskA >> taskB
