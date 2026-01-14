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
    dag_id='ETL_DWH_Daily',
    default_args=default_args,
    description='Run ETL DWH Daily',
    schedule_interval='0 2 * * *',  # Setiap jam 2 pagi
    start_date=pendulum.datetime(2025, 10, 30, tz="Asia/Jakarta"),  # Ganti dengan timezone Jakarta
    catchup=False,
    tags=['etl', 'stg', 'python'],
) as dag:

    # Test network connectivity first
    test_connection = BashOperator(
        task_id='test_sql_server_connection',
        bash_command='''
            echo "Testing SQL Server connectivity..."
            echo "Trying NB-ITAPS01..."
            nc -zv NB-ITAPS01 1433 2>&1 || echo "Failed to connect to NB-ITAPS01:1433"
            echo "Trying host.docker.internal..."
            nc -zv host.docker.internal 1433 2>&1 || echo "Failed to connect to host.docker.internal:1433"
            echo "Network test complete"
        '''
    )

    install_dependencies_tqdm = BashOperator(
        task_id='install_dependencies_tqdm',
        bash_command='pip install tqdm'
    )

    install_dependencies_pymsqql = BashOperator(
        task_id='install_dependencies_pymsqql',
        bash_command='pip install pymssql'
    )


    run_etl_STG = BashOperator(
        task_id='run_insert_bronze',
        bash_command='python /opt/airflow/scripts/STG_Insert_DWH.py'
    )

    run_etl_CORE = BashOperator(
        task_id='run_insert_silver',
        bash_command='python /opt/airflow/scripts/CORE_Insert_DWH.py'
    )

    run_etl_MART = BashOperator(
        task_id='run_insert_gold',
        bash_command='python /opt/airflow/scripts/MART_Insert_DWH.py'
    )    


    test_connection >> install_dependencies_tqdm  >>  install_dependencies_pymsqql >> run_etl_STG >> run_etl_CORE >> run_etl_MART
