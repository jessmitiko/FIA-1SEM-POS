from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 31),
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup': False,
    'retries': 1
}

dag = DAG('spark-submit-template', default_args=default_args, schedule_interval='@daily')

ingestion_template_job = SparkSubmitOperator(
    application="/home/user/bronze/jobs/ingestion-template.py", 
    task_id="ingestion-template",
    dag=dag
)

ingestion_template_job