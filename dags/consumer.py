import datetime

from airflow import DAG, Dataset
from airflow.decorators import task
from datasets import temp_file

with DAG(
    dag_id='consumer',
    schedule=[temp_file],
    start_date=datetime.datetime(2022, 11, 27),
    catchup=False,
):
    @task
    def read_dataset():
        with open(temp_file.uri, 'r')as file:
            print(file.read())

    read_dataset()
