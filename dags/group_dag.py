from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.download_subdags import downloads_dag
from subdags.transformation_subdags import transformation_dags
from datetime import datetime

with DAG('group_dag', start_date=datetime(2022, 1, 1),
         schedule_interval='@daily', catchup=False) as dag:

    downloads = SubDagOperator(
        task_id = 'downloads',
        subdag = downloads_dag(dag.dag_id, 'downloads',interval=dag.schedule_interval,
        start_date=dag.start_date,
        catchup=dag.catchup)
    )
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )

    transforms = SubDagOperator(
        task_id='transforms',
        subdag = transformation_dags(dag.dag_id, 'transforms', interval=dag.schedule_interval,
        start_time=dag.start_date,
        catchup=dag.catchup)
    )

    downloads >> check_files >> transforms