from airflow import DAG
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG('ingestion-posicao_linhas', catchup=False, default_args=default_args, schedule_interval='*/2 * * * *') #*/10 * * * *

ingestion_raw_posicao_by_linha = SparkSubmitOperator(
    application="/opt/airflow/jobs/bronze/ingestion-posicao_by_linha.py", 
    task_id="ingestion-raw-posicao_by_linha",
    packages="org.apache.hadoop:hadoop-aws:3.3.2",
    env_vars={
        "AWS_ACCESS_KEY_ID": "{{ var.value.AWS_ACCESS_KEY_ID }}",
        "AWS_SECRET_ACCESS_KEY": "{{ var.value.AWS_SECRET_ACCESS_KEY }}",
        "S3_ENDPOINT": "{{ var.value.S3_ENDPOINT }}"
    },
    dag=dag
)

ingestion_silver_posicao_by_linha = SparkSubmitOperator(
    application="/opt/airflow/jobs/silver/ingestion-posicao_by_linhas.py", 
    task_id="ingestion-silver-posicao_by_linha",
    packages="org.apache.hadoop:hadoop-aws:3.3.2",
    env_vars={
        "AWS_ACCESS_KEY_ID": "{{ var.value.AWS_ACCESS_KEY_ID }}",
        "AWS_SECRET_ACCESS_KEY": "{{ var.value.AWS_SECRET_ACCESS_KEY }}",
        "S3_ENDPOINT": "{{ var.value.S3_ENDPOINT }}"
    },
    dag=dag
)

final_gold_linhas_dag = TriggerDagRunOperator(
    task_id="final-gold-linhas_dag",
    trigger_dag_id="final-linhas",
    dag=dag
)

ingestion_raw_posicao_by_linha >> ingestion_silver_posicao_by_linha >> final_gold_linhas_dag