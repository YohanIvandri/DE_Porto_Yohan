from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum


default_args = {
    'owner': 'yohan',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_stock_daily',
    default_args=default_args,
    description='Run ETL Stock Daily',
    schedule_interval='0 2 * * *',  # Setiap jam 2 pagi
    start_date=pendulum.datetime(2025, 10, 30, tz="Asia/Jakarta"),  # Ganti dengan timezone Jakarta
    catchup=False,
    tags=['etl', 'bronze', 'python'],
) as dag:


    run_etl_bronze = BashOperator(
        task_id='run_insert_bronze',
        bash_command='python /opt/gcp_project/gcp_main_etl.py'
    )

    run_etl_bronze
