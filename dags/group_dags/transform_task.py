from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


def transform_tasks():
    with TaskGroup(group_id='transforms', tooltip='transform tasks') as group:
        transform_a = BashOperator(
            task_id='transform_a',
            bash_command='echo transforming something... && sleep 15'
        )
        transform_b = BashOperator(
            task_id='transform_b',
            bash_command='echo transforming something... && sleep 15'
        )
    return group
