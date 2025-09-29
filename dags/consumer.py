from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime
d_file = Dataset('/tmp/my_file.txt')

with DAG(dag_id='consumer', schedule=[d_file], start_date=datetime(2022,1,1), catchup=False):
    @task
    def read_dataset():
        with open (d_file.uri, 'r') as f:
            print(f.read())

    read_dataset()