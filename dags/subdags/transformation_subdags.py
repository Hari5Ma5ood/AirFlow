from airflow import DAG
from airflow.operators.bash import BashOperator

def transformation_dags(parent_dag_id, child_dag_id, **kwargs):
    with DAG(f"{parent_dag_id}.{child_dag_id}", start_date=kwargs['start_time'], schedule=kwargs['interval'],catchup=kwargs['catchup'] ) as dag:
        transform_a = BashOperator(
        task_id='transform_a',
        bash_command='sleep 10'
        )

        transform_b = BashOperator(
            task_id='transform_b',
            bash_command='sleep 10'
        )

        transform_c = BashOperator(
            task_id='transform_c',
            bash_command='sleep 10'
        )
    return dag