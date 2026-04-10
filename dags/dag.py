from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import os

# ===========================================
# Lấy thông tin từ Airflow Variables (Bạn đã tạo trên UI)
# ===========================================
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR")

# Các thông số Postgres để truyền vào Environment Variables
DBT_ENV = {
    **os.environ,
    "DBT_POSTGRES_HOST": Variable.get("DBT_POSTGRES_HOST"),
    "DBT_POSTGRES_USER": Variable.get("DBT_POSTGRES_USER"),
    "DBT_POSTGRES_PASSWORD": Variable.get("DBT_POSTGRES_PASSWORD"),
    "DBT_POSTGRES_PORT": Variable.get("DBT_POSTGRES_PORT"),
    "DBT_POSTGRES_DB": Variable.get("DBT_POSTGRES_DB"),
}

default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='dbt_final_execution_dag',
    default_args=default_args,
    description='Run dbt using Airflow Variables and Virtualenv',
    schedule=None,
    catchup=False,
    tags=['dbt', 'production', 'minikube']
) as dag:

    start = EmptyOperator(task_id='start')

    # Task thực thi dbt
    dbt_run = BashOperator(
        task_id='dbt_run_task',
        bash_command=f"""
            cd {DBT_PROJECT_DIR}
            
            echo "--- KIỂM TRA BIẾN MÔI TRƯỜNG ---"
            echo "Kết nối tới Host: $DBT_POSTGRES_HOST"
            
            # Chạy trực tiếp dbt (không cần venv vì Helm đã cài sẵn vào image)
            dbt debug --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --target dev
            
            if [ $? -eq 0 ]; then
                echo "--- KẾT NỐI THÀNH CÔNG, BẮT ĐẦU CHẠY DBT ---"
                dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --target dev
            else
                echo "--- THẤT BẠI KHI DEBUG ---"
                exit 1
            fi
        """,
        env=DBT_ENV
    )

    end = EmptyOperator(task_id='end')

    start >> dbt_run >> end