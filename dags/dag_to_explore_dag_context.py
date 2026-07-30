from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def print_context(**context):
    print("DAG Context:")
    for key, value in context.items():
        print(f"{key}: {value}")


default_args={
    'owner': 'airflow', 
    'retries': 5,
    'retry_delay': timedelta(minutes=2) 
}

with DAG(
        dag_id = 'dag_to_explore_dag_context',
        default_args = default_args,
        description = 'This is a DAG to explore DAG context',
        start_date=datetime(2026,3,18),
        schedule='@daily',
        catchup=False
) as dag:
    context_task = PythonOperator(
        task_id='context_task',
        python_callable=print_context,
    )

    notify_task = BashOperator(
        task_id='notify_task',  
        bash_command="echo 'DAG context has been printed'"
    )

    context_task >> notify_task