from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta

# Tái sử dụng cấu hình biến môi trường của bạn
dbt_env_vars = [
    k8s.V1EnvVar(name="DBT_POSTGRES_HOST", value="airflow-postgresql"),
    k8s.V1EnvVar(name="DBT_POSTGRES_USER", value="airflow"),
    k8s.V1EnvVar(name="DBT_POSTGRES_DB", value="airflow"),
    k8s.V1EnvVar(name="DBT_POSTGRES_PORT", value="5432"),
    k8s.V1EnvVar(name="DBT_POSTGRES_PASSWORD", value="{{ var.value.DBT_POSTGRES_PASSWORD }}"),
]

# Init Container để lấy code từ Git
git_sync_init = k8s.V1Container(
    name="git-sync-init",
    image="registry.k8s.io/git-sync/git-sync:v4.1.0",
    env=[
        k8s.V1EnvVar(name="GIT_SYNC_REPO", value="https://github.com/NamKhanh1811/dwh_banking.git"),
        k8s.V1EnvVar(name="GIT_SYNC_BRANCH", value="main"),
        k8s.V1EnvVar(name="GIT_SYNC_ROOT", value="/git"),
        k8s.V1EnvVar(name="GIT_SYNC_DEST", value="repo"),
        k8s.V1EnvVar(name="GIT_SYNC_ONE_TIME", value="true"),
    ],
    volume_mounts=[k8s.V1VolumeMount(name="dags-data", mount_path="/git")]
)

with DAG(
    dag_id='dbt_sor_workflow_k8s',
    start_date=datetime(2026, 4, 1),
    schedule_interval='0 2 * * *', # 3. Đặt lịch chạy 2h sáng hàng ngày
    catchup=False,
    tags=['dbt', 'sor', 'testing']
) as dag:

    # 1. Chạy ETL full luồng vùng SOR theo tag
    run_sor = KubernetesPodOperator(
        task_id="dbt_run_sor",
        name="dbt-run-sor",
        namespace="airflow",
        image="ghcr.io/dbt-labs/dbt-postgres:1.8.2",
        init_containers=[git_sync_init],
        cmds=["dbt"],
        arguments=[
            "run", 
            "--project-dir", "/opt/airflow/dags/repo", 
            "--profiles-dir", "/opt/airflow/dags/repo",
            "--select", "tag:dbt_sor", # Chạy theo tag
            "--target", "dev"
        ],
        env_vars=dbt_env_vars,
        volumes=[k8s.V1Volume(name="dags-data", empty_dir=k8s.V1EmptyDirVolumeSource())],
        volume_mounts=[k8s.V1VolumeMount(name="dags-data", mount_path="/opt/airflow/dags")],
        get_logs=True
    )

    # 2. Chạy dbt test và lưu kết quả vào bảng đích (--store-failures)
    test_sor = KubernetesPodOperator(
        task_id="dbt_test_sor",
        name="dbt-test-sor",
        namespace="airflow",
        image="ghcr.io/dbt-labs/dbt-postgres:1.8.2",
        init_containers=[git_sync_init],
        cmds=["dbt"],
        arguments=[
            "test", 
            "--project-dir", "/opt/airflow/dags/repo", 
            "--profiles-dir", "/opt/airflow/dags/repo",
            "--select", "tag:dbt_sor",
            "--store-failures", # Lưu kết quả test lỗi vào DB
            "--target", "dev"
        ],
        env_vars=dbt_env_vars,
        volumes=[k8s.V1Volume(name="dags-data", empty_dir=k8s.V1EmptyDirVolumeSource())],
        volume_mounts=[k8s.V1VolumeMount(name="dags-data", mount_path="/opt/airflow/dags")],
        get_logs=True
    )

    run_sor >> test_sor