from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

default_args = {'owner': 'bhas.neel'}

with DAG(
    dag_id='executing_multiple_tasks',
    description='DAG with multiple tasks and dependencies',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@once'
) as dag:

    taskA = BashOperator(
        task_id='taskA',
        bash_command='echo TASK A has been executed!'
    )

    taskB = BashOperator(
        task_id='taskB',
        bash_command='echo TASK B has been executed!'
    )

taskA.set_upstream(taskB)
