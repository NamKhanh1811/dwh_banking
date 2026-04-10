from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime

# 1. Khai báo biến môi trường để dbt kết nối DB
# Airflow sẽ truyền các biến này vào Pod dbt khi nó khởi tạo
dbt_env_vars = [
    k8s.V1EnvVar(name="DBT_POSTGRES_HOST", value="airflow-postgresql"),
    k8s.V1EnvVar(name="DBT_POSTGRES_USER", value="airflow"),
    k8s.V1EnvVar(name="DBT_POSTGRES_DB", value="airflow"),
    k8s.V1EnvVar(name="DBT_POSTGRES_PORT", value="5432"),
    k8s.V1EnvVar(name="DBT_POSTGRES_PASSWORD", value="{{ var.value.DBT_POSTGRES_PASSWORD }}"),
]

with DAG(
    dag_id='dbt_banking_k8s_final', # Tên DAG mới
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=['dbt', 'k8s', 'production']
) as dag:

    # Đây là Task thay thế cho run_dbt_models cũ của bạn
    run_dbt = KubernetesPodOperator(
        task_id="run_dbt_models",
        name="dbt-executor-pod",
        namespace="airflow",
        
        # SỬ DỤNG IMAGE CÓ SẴN DBT (Không cần pip install nữa)
        image="ghcr.io/dbt-labs/dbt-postgres:1.8.2",
        image_pull_policy="IfNotPresent",
        
        # LỆNH THỰC THI
        cmds=["dbt"],
        arguments=[
            "run", 
            "--project-dir", "/opt/airflow/dags/repo/banking_dwh", 
            "--profiles-dir", "/opt/airflow/dags/repo/banking_dwh",
            "--target", "dev"
        ],
        
        env_vars=dbt_env_vars,
        
        # GIỮ LẠI CẤU HÌNH GIT-SYNC (Để Pod này cũng thấy code giống Worker)
        # Nếu bạn dùng Helm Chart mặc định, thường chỉ cần mount volume 'dags'
        volumes=[
            k8s.V1Volume(
                name="dags-volume",
                empty_dir=k8s.V1EmptyDirVolumeSource() # Hoặc dùng hostPath nếu chạy Minikube
            )
        ],
        volume_mounts=[
            k8s.V1VolumeMount(
                name="dags-volume", 
                mount_path="/opt/airflow/dags", 
                read_only=True
            )
        ],
        
        get_logs=True,               # Xem log dbt ngay trên Airflow UI
        is_delete_operator_pod=True, # Chạy xong tự xóa Pod cho nhẹ Minikube
    )

    run_dbt