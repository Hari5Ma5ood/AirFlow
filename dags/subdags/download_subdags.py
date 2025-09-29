from sched import scheduler

from airflow.operators.bash import BashOperator
from airflow import DAG


def downloads_dag(parent_dag_id, child_dag_id, **kwargs):
    with DAG (f"{parent_dag_id}.{child_dag_id}", schedule=kwargs['interval'], start_date=kwargs['start_date'], catchup=kwargs['catchup']) as dag:
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

    return dag