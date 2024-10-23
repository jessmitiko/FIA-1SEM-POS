from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from airflow.operators.python import PythonOperator

from pprint import pprint

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

dag = DAG('dummy_operator_example', default_args=default_args, schedule_interval='@daily')

start_task = DummyOperator(task_id='start_task', dag=dag)

task_1 = DummyOperator(task_id='task_1', dag=dag)
task_2 = DummyOperator(task_id='task_2', dag=dag)

end_task = DummyOperator(task_id='end_task', dag=dag)

def print_context_func(**context):
    print(context)

print_context = PythonOperator(
    task_id="print_context",
    python_callable=print_context_func,
)

start_task >> task_1 >> task_2 >> end_task >> print_context