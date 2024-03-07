import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, BranchPythonOperator
from random import choice

default_args = {'owner': 'bhas.neel'}


def read_csv_file():
    df = pd.read_csv('/Users/bhaskarneella/airflow/datasets/insurance.csv')
    print(df)
    return df.to_json()


def has_driving_license():
    return choice([True, False])


def eligible_to_drive():
    print('Congrats! You\'re eligible to drive!!')


def ineligible_to_drive():
    print('Hard luck. You\'re ineligible to drive.')


def branch(ti):
    if ti.xcom_pull(task_ids='has_driving_license'):
        return 'eligible_to_drive'
    else:
        return 'ineligible_to_drive'


with DAG(
        dag_id='execute_python_branching',
        description='Running a branching pipeline',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['python', 'branching', 'pipeline']
) as dag:

    has_driving_license = PythonOperator(
        task_id='has_driving_license',
        python_callable=has_driving_license
    )

    licenseBranch = BranchPythonOperator(
        task_id='licenseBranch',
        python_callable=branch
    )

    eligible_to_drive = PythonOperator(
        task_id='eligible_to_drive',
        python_callable=eligible_to_drive
    )

    ineligible_to_drive = PythonOperator(
        task_id='ineligible_to_drive',
        python_callable=ineligible_to_drive
    )

has_driving_license >> licenseBranch >> [eligible_to_drive, ineligible_to_drive]
