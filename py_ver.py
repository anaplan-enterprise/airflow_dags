from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

def show_python_version():
    print(sys.version)

with DAG('check_python_version', start_date=datetime(2025,10,8), schedule_interval=None) as dag:
    task = PythonOperator(
        task_id='print_python_version',
        python_callable=show_python_version
    )
