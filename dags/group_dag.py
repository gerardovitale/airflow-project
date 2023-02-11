from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator

from group_dags.transform_task import transform_tasks
from sub_dags.donwload_dag import downloads

with DAG(
    'group_dag',
    start_date=datetime(2022, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    downloads = SubDagOperator(
        task_id='download',
        subdag=downloads(
            parent_dag_id=dag.dag_id,
            child_dag_id='download',
            args={
                'start_date': dag.start_date,
                'schedule_interval': dag.schedule_interval,
                'catchup': dag.catchup,
            }
        )
    )

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    transforms = transform_tasks()

    downloads >> check_files >> transforms
