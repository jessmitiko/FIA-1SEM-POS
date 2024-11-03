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

dag = DAG('spark-submit-template', default_args=default_args, schedule_interval='@daily')

ingestion_template_job = SparkSubmitOperator(
    application="/opt/airflow/jobs/spark-job-template.py", 
    task_id="ingestion-template",
    packages="org.apache.hadoop:hadoop-aws:3.2.0",
    env_vars={
        "AWS_ACCESS_KEY_ID": "{{ var.value.AWS_ACCESS_KEY_ID }}",
        "AWS_SECRET_ACCESS_KEY": "{{ var.value.AWS_SECRET_ACCESS_KEY }}",
        "S3_ENDPOINT": "{{ var.value.S3_ENDPOINT }}"
    },
    dag=dag
)

ingestion_template_job