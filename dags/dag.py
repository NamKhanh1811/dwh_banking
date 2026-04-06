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
        # 1. Cài đặt dbt (không dùng --user để tránh sai path)
        pip install dbt-core dbt-postgres --quiet
        
        # 2. Trỏ vào thư mục repo
        cd /opt/airflow/dags/repo
        
        # 3. Chạy dbt bằng cách gọi module python trực tiếp
        # Cách này sẽ bỏ qua lỗi "Interpreter không tồn tại" ở file bin
        python3 -m dbt.cli.main run \
            --project-dir . \
            --profiles-dir . \
            --target dev \
            --no-version-check
        """
    )