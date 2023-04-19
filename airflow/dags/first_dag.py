from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "me",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="first_dag_v2",
    default_args=default_args,
    description="First airflow dag",
    start_date=datetime(2023, 4, 19, 2),
    schedule_interval="@daily"
) as dag:
    task1 = BashOperator(
        task_id = "first_task",
        bash_command = "echo Hello world, this is the first task!"
    )

    task2 = BashOperator(
        task_id="second_task",
        bash_command="echo I am second task, Ill be running after T1"
    )

    task3 = BashOperator(
        task_id="third_task",
        bash_command="echo I am task T3"
    )

    task1 >> [task2, task3]    