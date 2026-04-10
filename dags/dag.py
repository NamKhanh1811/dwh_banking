from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime

dbt_env_vars = [
    k8s.V1EnvVar(name="DBT_POSTGRES_HOST", value="airflow-postgresql"),
    k8s.V1EnvVar(name="DBT_POSTGRES_USER", value="airflow"),
    k8s.V1EnvVar(name="DBT_POSTGRES_DB", value="airflow"),
    k8s.V1EnvVar(name="DBT_POSTGRES_PORT", value="5432"),
    k8s.V1EnvVar(name="DBT_POSTGRES_PASSWORD", value="{{ var.value.DBT_POSTGRES_PASSWORD }}"),
]

# Định nghĩa Init Container để clone code giống hệt worker
git_sync_init_container = k8s.V1Container(
    name="git-sync-init",
    image="registry.k8s.io/git-sync/git-sync:v4.1.0",
    env=[
        k8s.V1EnvVar(name="GIT_SYNC_REPO", value="https://github.com/NamKhanh1811/dwh_banking.git"),
        k8s.V1EnvVar(name="GIT_SYNC_BRANCH", value="main"),
        k8s.V1EnvVar(name="GIT_SYNC_ROOT", value="/git"),
        k8s.V1EnvVar(name="GIT_SYNC_DEST", value="repo"),
        k8s.V1EnvVar(name="GIT_SYNC_ONE_TIME", value="true"),
    ],
    volume_mounts=[
        k8s.V1VolumeMount(name="dags-data", mount_path="/git")
    ]
)

with DAG(
    dag_id='dbt_banking_k8s_final',
    start_date=datetime(2026, 4, 1),
    schedule_interval=None,
    catchup=False,
    tags=['dbt', 'k8s', 'production']
) as dag:

    run_dbt = KubernetesPodOperator(
        task_id="run_dbt_models",
        name="dbt-executor-pod",
        namespace="airflow",
        image="ghcr.io/dbt-labs/dbt-postgres:1.8.2",
        image_pull_policy="IfNotPresent",
        
        # Thêm Init Container vào đây
        init_containers=[git_sync_init_container],
        
        cmds=["dbt"],
        arguments=[
            "run", 
            "--project-dir", "/opt/airflow/dags/repo", 
            "--profiles-dir", "/opt/airflow/dags/repo",
            "--target", "dev"
        ],
        
        env_vars=dbt_env_vars,
        
        volumes=[
            k8s.V1Volume(
                name="dags-data",
                empty_dir=k8s.V1EmptyDirVolumeSource()
            )
        ],
        volume_mounts=[
            k8s.V1VolumeMount(
                name="dags-data", 
                # Lưu ý: mount vào đúng đường dẫn dbt tìm kiếm
                mount_path="/opt/airflow/dags", 
            )
        ],
        
        get_logs=True,
        is_delete_operator_pod=False,
    )

    run_dbt