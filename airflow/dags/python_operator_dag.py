from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "me",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}

def get_name(ti):
    ti.xcom_push(key="name", value="erik")
    ti.xcom_push(key="surname", value="cristian")

def get_age(ti):
    ti.xcom_push(key="age", value=31)

def greetings(ti):
    name = ti.xcom_pull(task_ids="get_name", key="name")
    surname = ti.xcom_pull(task_ids="get_name", key="surname")
    age = ti.xcom_pull(task_ids="get_age", key="age")
    print(f"Heya {name} {surname} {age}, dag with python")


with DAG(
    default_args=default_args,
    dag_id="dag_with_python_operator_v2",
    description="first dag with python",
    start_date=datetime(2023, 4, 19),
    schedule_interval="@daily",
) as dag:
    task1 = PythonOperator(
        task_id="greet",
        python_callable=greetings,        
    )

    task2 = PythonOperator(
        task_id = "get_name",
        python_callable=get_name,
    )

    task3 = PythonOperator(
        task_id="get_age",
        python_callable=get_age,
    )

    [task2, task3] >> task1