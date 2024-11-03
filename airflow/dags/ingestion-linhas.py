from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
    'retries': 1
}

dag = DAG('ingestion-linhas', default_args=default_args, schedule_interval='@daily')

ingestion_raw_terminal_linhas = SparkSubmitOperator(
    application="/opt/airflow/jobs/bronze/ingestion-linhas.py", 
    task_id="ingestion-raw-terminal_linhas",
    env_vars={
        "AWS_ACCESS_KEY_ID": "{{ var.value.AWS_ACCESS_KEY_ID }}",
        "AWS_SECRET_ACCESS_KEY": "{{ var.value.AWS_SECRET_ACCESS_KEY }}",
        "S3_ENDPOINT": "{{ var.value.S3_ENDPOINT }}"
    },
    dag=dag
)

ingestion_silver_terminal_linhas = SparkSubmitOperator(
    application="/opt/airflow/jobs/silver/ingestion-linhas.py", 
    task_id="ingestion-silver-terminal_linhas",
    packages="org.apache.hadoop:hadoop-aws:3.2.0,io.delta:delta-core_2.12:2.4.0",
    env_vars={
        "AWS_ACCESS_KEY_ID": "{{ var.value.AWS_ACCESS_KEY_ID }}",
        "AWS_SECRET_ACCESS_KEY": "{{ var.value.AWS_SECRET_ACCESS_KEY }}",
        "S3_ENDPOINT": "{{ var.value.S3_ENDPOINT }}"
    },
    dag=dag
)

ingestion_raw_terminal_linhas >> ingestion_silver_terminal_linhas