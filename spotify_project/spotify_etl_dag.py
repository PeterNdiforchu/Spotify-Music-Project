from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from dags import spotify_etl
from airflow.utils.dates import days_ago

my_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': [''],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
my_dag = DAG(
    'spotify_dag',
    default_args = my_args,
    description= 'Spotify ETL',
    schedule_interval= '0 14 * * *'
)


run_etl = PythonOperator(
    task_id='spotify_etl',
    python_callable= spotify_etl,
    dag=my_dag
)
run_etl
