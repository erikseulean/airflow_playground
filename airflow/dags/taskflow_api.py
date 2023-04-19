from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task

default_args = {
    "owner": "me",
    "retries": 5,
    "retry_delay": timedelta(minutes=2)
}

@dag(
    dag_id="dag_with_taskflow_api",
    start_date=datetime(2023, 4, 19),
    schedule_interval="@daily",
)
def hello_world_etl():

    @task()
    def get_name():
        return "Erik"
    
    @task()
    def get_age():
        return 31
    
    @task()
    def greet(name, age):
        print(f"Hello world {name} {age}")

    name = get_name()
    age = get_age()
    greet(name=name, age=age)

greet_dag = hello_world_etl()