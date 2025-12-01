from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args={
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='catchup_backfill_dag_v2',
    default_args=default_args,
    description='My first DAG',
    start_date=datetime(2025, 9, 19),
    schedule='@daily',
    catchup=False
) as dag:

    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo 'this is catchup task'"
    )