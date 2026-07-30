###############################################################################################################################################################################################################################
# This DAG demonstrates one way to handle late-arriving files in an S3 bucket. It uses the S3KeySensor to monitor the presence of a specific file in the bucket. Once the file is detected, it triggers a series of tasks:

# wait_for_s3_file: This task waits for the file landing/base_dataset.csv to appear in the S3 bucket glue-practice-31052025. It uses the S3KeySensor with a poke_interval of 5 seconds and a timeout of 5 minutes.

# copy_file: After the file is detected, this task copies the file from the landing/ folder to a processed/ folder with a date-based path. This ensures that each day's snapshot is preserved.

# notify_task: Finally, this task logs a message indicating that the file is now available.

# This approach is useful for ensuring that downstream processes only start after the required file is available, making it suitable for scenarios where file arrival times are unpredictable.

#########################################################################################################################################################################################################################################
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
import boto3
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

BUCKET  = "glue-practice-31052025"
LANDING = "landing/base_dataset.csv"   # HR team always uploads here

def msg():
    print("S3 file is now available!")

def copy_to_processed(ds: str, **context):
    """
    Copies base_dataset.csv from landing/ to a dated processed/ path.
    This preserves each day's snapshot before it gets overwritten tomorrow.
    """
    
    # CORRECT — S3Hook reads from Airflow Connection "aws_default"
    
    hook = S3Hook(aws_conn_id="aws_default")        
    s3   = hook.get_conn()
    

    source_key = LANDING
    dest_key   = f"processed/date={ds}/hr_records.csv"
    # → "processed/date=2026-03-17/hr_records.csv"

    # Copy from landing to processed (does not delete source)
    s3.copy_object(
        Bucket=BUCKET,
        CopySource={"Bucket": BUCKET, "Key": source_key},
        Key=dest_key,
    )
    print(f"Copied {source_key} → {dest_key}")

    # Read and print basic stats to confirm file is valid
    response = s3.get_object(Bucket=BUCKET, Key=dest_key)
    content  = response["Body"].read().decode("utf-8")
    lines    = content.strip().split("\n")

    print(f"File stats for {ds}:")
    print(f"  Total rows (incl header): {len(lines)}")
    print(f"  Header: {lines[0]}")
    print(f"  First record: {lines[1] if len(lines) > 1 else 'empty'}")
    print(f"  Destination: s3://{BUCKET}/{dest_key}")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="s3_sensor_example",
    start_date=datetime(2026, 3, 19),
    schedule="0 4 * * *",
    catchup=False,
    default_args=default_args,
    tags=["s3"],
) as dag:

    wait_for_s3_file = S3KeySensor(
        task_id="wait_for_s3_file",
        bucket_key="landing/base_dataset.csv",   # key or path inside bucket
        bucket_name="glue-practice-31052025",       # your S3 bucket
        aws_conn_id="aws_default",     # defined in Airflow Connections
        poke_interval=5,              # check every 5 seconds
        timeout=300,               # stop after 5 minutes
        mode="reschedule"             # free up worker slot while waiting
    )

    copy_file = PythonOperator(
        task_id="copy_file",
        python_callable=copy_to_processed,
        op_kwargs={"ds": "{{ ds }}"}  # Pass execution date as string
    )

    notify_task = PythonOperator(
        task_id= "notify_task",
        python_callable=msg
    )

    wait_for_s3_file >> copy_file >> notify_task
