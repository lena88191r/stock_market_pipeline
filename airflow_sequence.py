from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine


def extract_data(fetch_data):

    pass

def transform_data(clean_data):

    pass

def load_data(database_connection):
    # Load to database
    pass

# Define the DAG
with DAG(
    'stock_etl_pipeline',
    default_args={'start_date': datetime(2024, 1, 1)},
    schedule_interval='@daily',
) as dag:
    extract = PythonOperator(task_id='extract', python_callable=extract_data)
    transform = PythonOperator(task_id='transform', python_callable=transform_data)
    load = PythonOperator(task_id='load', python_callable=load_data)

    extract >> transform >> load
