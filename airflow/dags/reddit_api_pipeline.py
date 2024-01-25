from airflow import DAG
from airflow.operators.python import PythonOperator
from reddit_api_pipeline_project.reddit_access import reddit_access
from reddit_api_pipeline_project.download_data import data_export
from reddit_api_pipeline_project.pipeline_all_utils import s3_upload
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime, timedelta

# Define the default_args
default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "georgiuandrei05@gmail.com"
}

# Define the DAG
with DAG(
        dag_id='reddit_api',
        description='Testing the Data Forex Pipeline from Udemy Course',
        default_args=default_args,
        schedule_interval="@daily",
        start_date=datetime(2024, 1, 1),
        catchup=False
) as dag:
    create_reddit_token_access = PythonOperator(
        task_id="create_reddit_token_access",
        python_callable=reddit_access.main
    )

    download_reddit_data = PythonOperator(
        task_id="download_reddit_data",
        python_callable=data_export.main
    )

    upload_data_to_s3 = PythonOperator(
        task_id="upload_data_to_s3",
        python_callable=s3_upload.main
    )

    process_data_with_spark = SparkSubmitOperator(
        task_id="process_data_with_spark",
        conn_id="spark-conn",
        application="/opt/airflow/jobs/python/spark_reddit_api_processing.py",
        packages='com.amazonaws:aws-java-sdk:1.12.262,org.apache.hadoop:hadoop-aws:3.3.4',
        verbose=False,
    )

    create_reddit_token_access >> download_reddit_data >> upload_data_to_s3 >> process_data_with_spark