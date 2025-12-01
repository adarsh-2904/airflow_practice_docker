from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta





def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    name = f"{first_name} {last_name}"
    age= ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello I am {name} and I am {age} years old")

def get_name(ti):
    ti.xcom_push(key='first_name', value='Jerry')
    ti.xcom_push(key='last_name', value='Smith')

def get_age(ti):
    ti.xcom_push(key='age', value=20)


default_args={
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='dag_with_python_op',
    default_args=default_args,
    description='This is python operator dag',
    start_date=datetime(2025, 9, 20),
    schedule='@daily',
    catchup=False
) as dag:
    task1 = PythonOperator(
        task_id='greet_task',       
        python_callable=greet,
        #op_kwargs={'age': 5}
    )
    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )
    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

    [task2,task3] >> task1 