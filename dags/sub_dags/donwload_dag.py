from typing import Any, Dict

from airflow import DAG
from airflow.operators.bash import BashOperator


def downloads(parent_dag_id: str, child_dag_id: str, args: Dict[str, Any]):
    with DAG(
        dag_id=f"{parent_dag_id}.{child_dag_id}",
        start_date=args['start_date'],
        schedule_interval=args['schedule_interval'],
        catchup=args['catchup']
    ) as dag:
        download_a = BashOperator(
            task_id='download_a',
            bash_command='echo downloading something... && sleep 15'
        )
        download_b = BashOperator(
            task_id='download_b',
            bash_command='echo downloading something... && sleep 15'
        )
    return dag
