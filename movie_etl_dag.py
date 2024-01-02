from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, avg, max, min, count, col
import process_movie_data
# Function to perform PySpark ETL operations

# process_movie_data()


# Define the Airflow DAG
dag = DAG(
    'movie_etl_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None  # Set your desired schedule interval
)

# Define the ETL task
etl_task = PythonOperator(
    task_id='process_movie_data',
    python_callable=process_movie_data,
    dag=dag
)

