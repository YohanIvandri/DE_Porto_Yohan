from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
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
    description='Run ETL Stock Daily',
    schedule='0 2 * * *',
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    tags=['etl', 'bronze'],
) as dag:

    run_etl_bronze = BashOperator(
        task_id='run_insert_bronze',
        bash_command='python /home/airflow/gcs/dags/gcp_main_etl.py'
    )
