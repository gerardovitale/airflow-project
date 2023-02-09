import datetime
import json

from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pandas import json_normalize


def _process_user(ti: TaskInstance) -> None:
    user = ti.xcom_pull(task_ids='extract_user')
    user = user['results'][0]
    processed_user = json_normalize({
        'firstname': user['name']['first'],
        'lastname': user['name']['last'],
        'country': user['location']['country'],
        'username': user['login']['username'],
        'password': user['login']['password'],
        'emial': user['email'],
    })
    processed_user.to_csv('/tmp/processed_user.csv', header=False, index=False)


def _store_user() -> None:
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY users FROM stdin DELIMITER as ','",
        filename='/tmp/processed_user.csv',
    )


with DAG(
    dag_id='user_processing',
    schedule='@daily',
    start_date=datetime.datetime(2022, 11, 26, 22, 15),
    catchup=False,  # no executed until the scheduled date
) as dag:
    # task_ids must be unique in the scope of a DAG
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS users (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
        ''',
    )
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/',
    )
    extract_user = SimpleHttpOperator(
        task_id='extract_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda res: json.loads(res.text),
        log_response=True,
    )
    process_user = PythonOperator(task_id='process_user', python_callable=_process_user)
    store_user = PythonOperator(task_id='store_user', python_callable=_store_user)

    create_table >> is_api_available >> extract_user >> process_user >> store_user
