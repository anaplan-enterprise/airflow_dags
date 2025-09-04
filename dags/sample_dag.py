from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# Define the DAG
with DAG(
    dag_id="print_name_dag",
    start_date=pendulum.datetime(2025, 9, 1, tz="UTC"),
    catchup=False,
    schedule="@once",
    tags=["example", "tutorial"],
) as dag:
    # Define the task
    print_name_task = BashOperator(
        task_id="print_my_name_task",
        bash_command='echo "Hello, Nagpritam Naik!"',
    )
