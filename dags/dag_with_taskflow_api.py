from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args={
    'owner': 'adarsh',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id='dag_with_taskflow_api',
    default_args=default_args,
    description='This is taskflow api dag',
    start_date=datetime(2025, 9, 22),
    schedule='@daily')
def my_dag():

    @task
    def greet(first_name:str,last_name:str, age: int):
        print(f"Hello I am {first_name} {last_name} and I am {age} years old")

    @task(multiple_outputs=True)
    def get_name() -> dict:
        return {'first_name': 'Jerry', 'last_name': 'Smith'}

    @task
    def get_age() -> int:
        return 20

    name_dict = get_name()
    age = get_age()
    greet(first_name=name_dict['first_name'],last_name=name_dict['last_name'], age=age)

greet_dag = my_dag()

