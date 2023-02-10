from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    'parallel_dag',
    start_date=datetime(2022, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    extract_a = BashOperator(
        task_id='extract_a',
        bash_command='sleep 10 && echo "$(date +T%H:%M:%S)"'
    )

    extract_b = BashOperator(
        task_id='extract_b',
        bash_command='sleep 10 && echo "$(date +T%H:%M:%S)"'
    )

    load_a = BashOperator(
        task_id='load_a',
        bash_command='sleep 10 && echo "$(date +T%H:%M:%S)"'
    )

    load_b = BashOperator(
        task_id='load_b',
        bash_command='sleep 10 && echo "$(date +T%H:%M:%S)"'
    )

    transform = BashOperator(
        task_id='transform',
        bash_command='sleep 10 && echo "$(date +T%H:%M:%S)"'
    )

    extract_a >> load_a
    extract_b >> load_b
    [load_a, load_b] >> transform
