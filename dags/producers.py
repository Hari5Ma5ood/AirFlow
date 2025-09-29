from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

d_file = Dataset('/tmp/my_file.txt')

with DAG("producers", schedule='@daily', start_date=datetime(2022,1,1), catchup=False):
    @task(outlets=[d_file])
    def update_dataset():
        with open (d_file.uri, 'a+') as f:
            f.write("its been updated for the fist time")

    update_dataset()