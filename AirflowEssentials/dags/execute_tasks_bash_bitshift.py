from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator

default_args = {'owner': 'bhas.neel'}

with DAG(
        dag_id='executing_multiple_tasks_bit_shift',
        description='DAG with multiple tasks and dependencies',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once'
) as dag:
    taskA = BashOperator(
        task_id='taskA',
        bash_command='''
            echo TASK A has started!
            for i in {1..10}
            do
                echo TASK A printing $i
            done
            echo TASK A has ended!
        '''
    )

    taskB = BashOperator(
        task_id='taskB',
        bash_command='''
            echo TASK B has started!
            sleep 4
            echo TASK B has ended!
        '''
    )

    taskC = BashOperator(
        task_id='taskC',
        bash_command='''
            echo TASK C has started!
            sleep 8
            echo TASK C has ended!
        '''
    )

    taskD = BashOperator(
        task_id='taskD',
        bash_command='echo TASK D has been executed!'
    )

taskA >> [taskB, taskC]
taskD << [taskB, taskC]
