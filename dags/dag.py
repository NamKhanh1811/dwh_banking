from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import os

# ===========================================
# Airflow Variables Configuration
# ===========================================
# Đảm bảo bạn đã tạo các Variable này trên Airflow UI
# ===========================================

# Lấy thông tin từ Airflow Variables
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR", default_var="/opt/airflow/dags/repo")
# Chú ý: Thư mục chứa profiles.yml thường nằm cùng project dir trong repo của bạn
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR", default_var="/opt/airflow/dags/repo")

default_args = {
    'owner': 'data-platform',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='dbt_transformation_v2',
    default_args=default_args,
    description='Run dbt models with virtualenv isolation',
    schedule=None,
    catchup=False,
    tags=['dbt', 'postgres', 'isolated']
) as dag:

    start = EmptyOperator(task_id='start')

    # Task chạy dbt: Tự động tạo venv, cài dbt và chạy model
    dbt_run = BashOperator(
        task_id='dbt_run_task',
        bash_command=f"""
            # 1. Tạo môi trường ảo riêng biệt để tránh xung đột Protobuf
            python3 -m venv /tmp/dbt_venv && \
            source /tmp/dbt_venv/bin/activate && \
            
            # 2. Cài đặt các thư viện cần thiết bản ổn định
            pip install --quiet --no-cache-dir \
                dbt-core==1.8.0 \
                dbt-postgres==1.8.0 \
                psycopg2-binary && \
            
            # 3. Di chuyển vào thư mục project
            cd {DBT_PROJECT_DIR} && \
            
            # 4. Xóa rác (target/logs) từ Windows nếu có
            rm -rf target/ logs/ && \
            
            # 5. Thực thi dbt
            python3 -m dbt.cli.main run \
                --project-dir . \
                --profiles-dir {DBT_PROFILES_DIR} \
                --target dev \
                --no-version-check
        """,
        # Airflow sẽ tự động truyền các biến môi trường vào nếu bạn cần dùng trong profiles.yml
        env={
            **os.environ,
            "DBT_POSTGRES_HOST": Variable.get("DBT_POSTGRES_HOST", "192.168.49.1"),
            "DBT_POSTGRES_USER": Variable.get("DBT_POSTGRES_USER", "postgres"),
            "DBT_POSTGRES_PW": Variable.get("DBT_POSTGRES_PASSWORD", "khanh181106"),
            "DBT_POSTGRES_PORT": Variable.get("DBT_POSTGRES_PORT", "5432"),
            "DBT_POSTGRES_DB": Variable.get("DBT_POSTGRES_DB", "postgres"),
        }
    )

    end = EmptyOperator(task_id='end')

    start >> dbt_run >> end