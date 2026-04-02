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
        python3 -m pip install --user dbt-core dbt-postgres && 
        export PATH=$PATH:/home/airflow/.local/bin &&
        cd /opt/airflow/dags/repo && 
        dbt run --profiles-dir .
        """
    )