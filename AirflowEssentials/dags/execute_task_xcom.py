from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

default_args = {'owner': 'bhas.neel'}


def increment_by_1(counter):
    print('Count, {counter}!'.format(counter=counter))
    return counter+1


def multiply_by_100(counter):
    print('Count, {counter}!'.format(counter=counter))
    return counter*100


with DAG(
        dag_id='execute_tasks_xcom',
        description='Cross task communication with XCOM',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@daily',
        tags=['xcom', 'python']
) as dag:
    taskA = PythonOperator(
        task_id='increment_by_1',
        python_callable=increment_by_1,
        op_kwargs={'counter': 100}
    )

    taskB = PythonOperator(
        task_id='multiply_by_100',
        python_callable=multiply_by_100,
        op_kwargs={'counter': 9}
    )

taskA >> taskB
