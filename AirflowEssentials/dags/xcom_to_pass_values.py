from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

default_args = {'owner': 'bhas.neel'}


def increment_by_1(counter):
    print('Count, {counter}!'.format(counter=counter))
    return counter+1


def multiply_by_100(ti):
    value = ti.xcom_pull(task_ids='increment_by_1')
    print('Value: {value}!'.format(value=value))
    return value*100


def print_value(ti):
    value = ti.xcom_pull(task_ids='multiply_by_100')
    print('Value: {value}!'.format(value=value))


with DAG(
        dag_id='execute_xcom_to_pass_values',
        description='Cross task communication with XCOM',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@daily',
        tags=['xcom', 'pass', 'python']
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

    taskC = PythonOperator(
        task_id='print_value',
        python_callable=print_value
    )

taskA >> taskB >> taskC
