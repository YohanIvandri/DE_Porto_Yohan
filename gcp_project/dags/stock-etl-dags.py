from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import pendulum
import sys

# Set path SEBELUM import
sys.path.insert(0, '/opt/airflow/gcp_project')

from src.ingestion_bronze import save_to_bronze

default_args = {
    'owner': 'yohan',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_bronze_ingestion():
    """Function untuk run ETL bronze"""
    tickers = ['NVDA', 'GOOGL', 'AAPL']
    bucket_name = 'stock-etl-bronze'
    
    for ticker in tickers:
        save_to_bronze(ticker, bucket_name)

with DAG(
    dag_id='etl_stock_daily',
    default_args=default_args,
    description='Run ETL Stock Daily',
    schedule='0 2 * * *',
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Jakarta"),
    catchup=False,
    tags=['etl', 'bronze'],
) as dag:

    bronze_task = PythonOperator(
        task_id='run_insert_bronze',
        python_callable=run_bronze_ingestion
    )