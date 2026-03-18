from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator



def msg():
    print("landing to staging job completed!")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}
with DAG(
    'glue_job_trigger',
    default_args=default_args,
    start_date=datetime(2025,12,10),
    schedule = '@daily',
    catchup=False,
) as dag:

    landing_to_staging_glue_job = GlueJobOperator(
        task_id="landing_to_staging_glue_job",
        job_name="landing_to_staging",
        script_location=f"s3://aws-glue-assets-775692714127-ap-south-1/scripts/landing_to_staging.py",
        s3_bucket="aws-glue-assets-775692714127-ap-south-1",
        iam_role_name="AWSGlueServiceRole",
        create_job_kwargs={"GlueVersion": "5.0", "NumberOfWorkers": 2, "WorkerType": "G.1X"},
        script_args = {
            '--file_name': 'delta_dataset'
        }
    )

    staging_to_curated_glue_job = GlueJobOperator(
        task_id="staging_to_curated_glue_job",
        job_name="staging_to_curated",
        script_location = f"s3://aws-glue-assets-775692714127-ap-south-1/scripts/staging_to_curated.py",
        s3_bucket = "aws-glue-assets-775692714127-ap-south-1",
        iam_role_name="AWSGlueServiceRole",
        create_job_kwargs={"GlueVersion": "5.0", "NumberOfWorkers": 2, "WorkerType": "G.1X"}
    )

    notify_task = PythonOperator(
        task_id='notify_task',
        python_callable=msg
    )

    landing_to_staging_glue_job >> staging_to_curated_glue_job >> notify_task