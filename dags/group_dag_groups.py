from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.download_subdags import downloads_dag
from subdags.transformation_subdags import transformation_dags
from datetime import datetime

from groups.download_groups import download_group
from groups.transformation_groups import transformation_groups

with DAG('group_dag_group', start_date=datetime(2022, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:

    downloads = download_group()

    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    transforms = transformation_groups()

    downloads >> check_files >> transforms