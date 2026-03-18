from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator


def msg():
    print("S3 file is now available!")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="s3_sensor_example",
    start_date=datetime(2025, 11, 6),
    schedule='@daily',
    catchup=False,
    default_args=default_args,
    tags=["s3"],
) as dag:

    wait_for_s3_file = S3KeySensor(
        task_id="wait_for_s3_file",
        bucket_key="landing/base_dataset.csv",   # key or path inside bucket
        bucket_name="glue-practice-31052025",       # your S3 bucket
        aws_conn_id="aws_default",     # defined in Airflow Connections
        poke_interval=60,              # check every 60 seconds
        timeout=60 * 60,               # stop after 1 hour
        mode="poke"
    )
    notify_task = PythonOperator(
        task_id= "notify_task",
        python_callable=msg
    )

    wait_for_s3_file >> notify_task