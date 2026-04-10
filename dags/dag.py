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
            # 1. Cài đặt trực tiếp vào virtualenv hiện tại (Bỏ --user)
            echo "--- ĐANG CÀI ĐẶT DBT ---"
            python3 -m pip install dbt-postgres --no-cache-dir
            
            # 2. Lấy đường dẫn tuyệt đối của dbt vừa cài
            DBT_EXE=$(python3 -c "import site; import os; print(os.path.join(site.getsitepackages()[0], '../../../bin/dbt'))")
            # Nếu lệnh trên phức tạp quá, ta thử dùng lệnh 'which'
            [ -f "$DBT_EXE" ] || DBT_EXE=$(which dbt)
            
            cd {DBT_PROJECT_DIR} || exit 1
            
            echo "--- KIỂM TRA PHIÊN BẢN ---"
            $DBT_EXE --version
            
            echo "--- THỰC THI DEBUG ---"
            $DBT_EXE --debug debug --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --target dev --no-use-colors
            
            if [ $? -eq 0 ]; then
                echo "--- KẾT NỐI THÀNH CÔNG ---"
                $DBT_EXE run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --target dev
            else
                echo "--- THẤT BẠI KHI DEBUG ---"
                exit 1
            fi
        """,
        env=DBT_ENV
    )

    end = EmptyOperator(task_id='end')

    start >> dbt_run >> end