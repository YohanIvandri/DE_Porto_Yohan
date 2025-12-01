from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'yohan',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='test_dag_hello',
    default_args=default_args,
    description='DAG test pertama',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    task_hello = BashOperator(
        task_id='print_hello',
        bash_command='echo "Hello Airflow, ini DAG pertama Yohan!"'
    )

    task_hello2 = BashOperator(
        task_id='print_hello2',
        bash_command='echo "Hello Airflow, ini DAG pertama Yohan!"'
    )

    task_hello3 = BashOperator(
        task_id='print_hello3',
        bash_command='echo "Hello Airflow, ini DAG pertama Yohan!"'
    )

    task_hello4 = BashOperator(
        task_id='print_hello4',
        bash_command='echo "Hello Airflow, ini DAG pertama Yohan!"'
    )

    task_hello5 = BashOperator(
        task_id='print_hello5',
        bash_command='echo "Hello Airflow, ini DAG pertama Yohan!"'
    )   


    task_hello >> task_hello2
    task_hello3 >> task_hello5
    task_hello4
    
