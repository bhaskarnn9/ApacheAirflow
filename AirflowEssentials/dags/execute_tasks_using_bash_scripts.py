import os
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

default_args = {'owner': 'bhas.neel'}

with DAG(
        dag_id='executing_tasks_using_bash_scripts',
        description='DAG with multiple tasks in bash scripts',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval=timedelta(days=1),
        tags=['scripts', 'template_search'],
        template_searchpath='/Users/bhaskarneella/airflow/dags/bash_scripts'

) as dag:

    taskA = BashOperator(
        task_id='taskA',
        bash_command='taskA.sh'
    )

    taskB = BashOperator(
        task_id='taskB',
        bash_command='taskB.sh'
    )

    taskC = BashOperator(
        task_id='taskC',
        bash_command='taskC.sh'
    )

    taskD = BashOperator(
        task_id='taskD',
        bash_command='taskD.sh'
    )

    taskE = BashOperator(
        task_id='taskE',
        bash_command='taskE.sh'
    )

    taskF = BashOperator(
        task_id='taskF',
        bash_command='taskF.sh'
    )

    taskG = BashOperator(
        task_id='taskG',
        bash_command='taskG.sh'
    )

taskA >> taskB >> taskE
taskA >> taskC >> taskF
taskA >> taskD >> taskG
