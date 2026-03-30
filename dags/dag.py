from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='my_dbt_transformation',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    run_dbt = BashOperator(
        task_id='dbt_run_task',
        bash_command="""
        cd /opt/airflow/dags &&
        dbt debug &&
        dbt run
        """
    )