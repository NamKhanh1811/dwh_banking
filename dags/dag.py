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
            # 1. Tạo môi trường ảo nếu chưa có và kích hoạt
            python3 -m venv /tmp/dbt_venv
            source /tmp/dbt_venv/bin/activate
            
            # 2. Chỉ cài đặt nếu chưa có dbt (tăng tốc độ chạy)
            pip install --quiet --no-cache-dir dbt-core==1.8.0 dbt-postgres==1.8.0 psycopg2-binary==2.9.9
            
            # 3. Di chuyển vào thư mục dự án
            cd {DBT_PROJECT_DIR} || {{ echo "LỖI: Không tìm thấy thư mục dự án tại {DBT_PROJECT_DIR}"; exit 2; }}
            
            # 4. Kiểm tra sự tồn tại của file quan trọng trước khi chạy
            if [ ! -f "dbt_project.yml" ]; then
                echo "LỖI: Không tìm thấy file dbt_project.yml trong $(pwd)"
                ls -la
                exit 2
            fi

            # 5. Chạy dbt debug để kiểm tra kết nối trước, sau đó mới run
            echo "--- ĐANG KIỂM TRA KẾT NỐI (DBT DEBUG) ---"
            python3 -m dbt.cli.main debug --project-dir . --profiles-dir {DBT_PROFILES_DIR} --target dev --no-version-check
            
            echo "--- ĐANG CHẠY DỰ ÁN (DBT RUN) ---"
            python3 -m dbt.cli.main run --project-dir . --profiles-dir {DBT_PROFILES_DIR} --target dev --no-version-check
        """,
        env=DBT_ENV
    )

    end = EmptyOperator(task_id='end')

    start >> dbt_run >> end