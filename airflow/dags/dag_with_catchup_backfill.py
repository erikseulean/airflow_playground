from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "me",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}


with DAG(
    dag_id="backfill_dag_v1",
    default_args=default_args,
    start_date=datetime(2023, 4, 15),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task1 = BashOperator(
        task_id="task1",
        bash_command = "echo This is the first dag!"
    )