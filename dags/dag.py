from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='my_dbt_transformation',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command="""
        cd /opt/airflow/dags/dbt_project &&
        dbt debug
        """
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command="""
        cd /opt/airflow/dags/dbt_project &&
        dbt run
        """
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command="""
        cd /opt/airflow/dags/dbt_project &&
        dbt test
        """
    )

    dbt_debug >> dbt_run >> dbt_test