from airflow import DAG
from datetime import datetime
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG('final-linhas', catchup=False, default_args=default_args, schedule_interval=None)

final_gold_linhas = SparkSubmitOperator(
    application="/opt/airflow/jobs/gold/final-linhas.py", 
    task_id="final-gold-linhas",
    packages="org.apache.hadoop:hadoop-aws:3.3.2",
    env_vars={
        "AWS_ACCESS_KEY_ID": "{{ var.value.AWS_ACCESS_KEY_ID }}",
        "AWS_SECRET_ACCESS_KEY": "{{ var.value.AWS_SECRET_ACCESS_KEY }}",
        "S3_ENDPOINT": "{{ var.value.S3_ENDPOINT }}"
    },
    dag=dag
)

final_gold_linhas