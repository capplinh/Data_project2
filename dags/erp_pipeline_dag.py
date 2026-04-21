from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner":            "data_engineer",
    "retries":          3,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="erp_daily_pipeline",
    default_args=default_args,
    description="ERP Data Pipeline — Bronze → Silver → Gold",
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["erp", "medallion", "daily"],
) as dag:

    bronze_load = BashOperator(
        task_id="bronze_incremental_load",
        bash_command="echo 'Bronze incremental load — OK'",
    )

    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command="echo 'Silver transform completed'",
    )

    gold_transform = BashOperator(
        task_id="gold_transform",
        bash_command="echo 'Gold transform completed'",
    )

    quality_check = BashOperator(
        task_id="data_quality_check",
        bash_command="echo 'Quality check completed'",
    )

    pipeline_done = BashOperator(
        task_id="pipeline_done",
        bash_command='echo "Pipeline completed at $(date)"',
    )

    bronze_load >> silver_transform >> gold_transform >> quality_check >> pipeline_done