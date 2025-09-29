from sched import scheduler

from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow import DAG


def download_group():
    with TaskGroup('downloads', tooltip='download task') as group:
        download_a = BashOperator(
            task_id='download_a',
            bash_command='sleep 10'
        )

        download_b = BashOperator(
            task_id='download_b',
            bash_command='sleep 10'
        )

        download_c = BashOperator(
            task_id='download_c',
            bash_command='sleep 10'
        )

    return group