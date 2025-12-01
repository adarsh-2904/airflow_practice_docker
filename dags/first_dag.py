from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args={
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='first_dag',
    default_args=default_args,
    description='My first DAG',
    start_date=datetime(2025, 9, 17),
    schedule='@daily',
    catchup=False
) as dag:

    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo 'this is my first task'"
    )

    task2 = BashOperator(
        task_id='sleep',
        bash_command="echo hey, I am task2 and will be running after task1!"
    )

    task3 = BashOperator(
        task_id='print_hello',
        bash_command='echo hey, I am task3 and will be running after task1 at the same time as task2!'
    )

    task1 >> [task2, task3]