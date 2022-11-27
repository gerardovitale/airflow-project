import datetime

from airflow import DAG, Dataset
from airflow.decorators import task
from datasets import temp_file

with DAG(
    dag_id='producer',
    schedule='@daily',
    start_date=datetime.datetime(2022, 11, 27),
    catchup=False,
):
    @task(outlets=[temp_file])
    def update_dataset():
        with open(temp_file.uri, 'a+')as file:
            file.write('[{0}] - this is a producer update'.format(
                datetime.datetime.now().isoformat()))

    update_dataset()
