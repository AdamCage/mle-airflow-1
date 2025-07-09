# dags/alt_churn.py

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from steps.churn import create_table, extract, transform, load
from steps.messages import send_telegram
from datetime import datetime

with DAG(
    dag_id='alt_churn',
    start_date=datetime(2023, 1, 1),
    schedule='@once',
    on_failure_callback=send_telegram
) as dag:

    create_table_step = PythonOperator(task_id='create_table', python_callable=create_table)
    extract_step = PythonOperator(task_id='extract', python_callable=extract)
    transform_step = PythonOperator(task_id='transform', python_callable=transform)
    load_step = PythonOperator(task_id='load', python_callable=load)

    create_table_step >> extract_step >> transform_step >> load_step