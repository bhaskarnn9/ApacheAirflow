import time
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

default_args = {'owner': 'bhas.neel'}


def task_a():
    print('task A executed!')


def task_b():
    time.sleep(5)
    print('task B executed!')


def task_c():
    print('task C executed!')


def task_d():
    print('task D executed!')


with DAG(
        dag_id='execute_tasks_python_operators',
        description='DAGs using python operator',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@daily',
        tags=['dependencies', 'python']
) as dad:
    taskA = PythonOperator(
        task_id='taskA',
        python_callable=task_a
    )

    taskB = PythonOperator(
        task_id='taskB',
        python_callable=task_b
    )

    taskC = PythonOperator(
        task_id='taskC',
        python_callable=task_c
    )

    taskD = PythonOperator(
        task_id='taskD',
        python_callable=task_d
    )

taskA >> [taskB, taskC]
[taskB, taskC] >> taskD
