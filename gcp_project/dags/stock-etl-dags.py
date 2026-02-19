from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum

default_args = {
    'owner': 'yohan',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_stock_daily',
    default_args=default_args,
    description='Run Full ETL Pipeline (Bronze + Silver)',
    schedule='0 2 * * *',
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    tags=['etl', 'pipeline'],
) as dag:

    run_full_etl = BashOperator(
        task_id="run_pipeline",
        bash_command="python3 /opt/airflow/gcp_project/gcp_main_etl.py"
    )
