from ETL.transform import transformation
from ETL.extract import download_dataset, read_file
from ETL.load import insert_data_from_df
from airflow.operators.python import PythonOperator
from ETL.dags import *
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
from datetime import datetime


# Default Arguments for DAG
default_args = {
    'owner': 'toqa',
}
# DAG Definition
dag = DAG(
    dag_id='ETL_pipeline',
    default_args=default_args,
    description='Simple ETL pipeline with Airflow',
    schedule_interval='@daily',
    start_date=datetime(2024, 12, 14),
    catchup=False
)


  
# Tasks
# 1. Download File Task
download_file = PythonOperator(
    task_id='download_file',
    python_callable=download_dataset,
    dag=dag
)

reading_file = PythonOperator(
    task_id='read_file',
    python_callable=read_file,
    dag=dag
)
# 2. Transformation Task
transform_file_step = PythonOperator(
    task_id='transform_file',
    python_callable=transformation,
    dag=dag
)

# 3. Create Table Task
create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="postgres_load",
    sql="""
    CREATE TABLE IF NOT EXISTS student (
        gender VARCHAR(50) NOT NULL,
        age DOUBLE PRECISION NOT NULL,
        city VARCHAR(100) NOT NULL,
        profession VARCHAR(100) NOT NULL,
        academic_pressure DOUBLE PRECISION NOT NULL,
        work_pressure DOUBLE PRECISION NOT NULL,
        cgpa DOUBLE PRECISION NOT NULL,
        study_satisfaction DOUBLE PRECISION NOT NULL,
        job_satisfaction DOUBLE PRECISION NOT NULL,
        sleep_duration VARCHAR(50) NOT NULL,
        dietary_habits VARCHAR(50) NOT NULL,
        degree VARCHAR(100) NOT NULL,
        suicidal_thoughts VARCHAR(5) NOT NULL,
        work_study_hours DOUBLE PRECISION NOT NULL,
        financial_stress DOUBLE PRECISION,
        family_history VARCHAR(5) NOT NULL,
        depression INTEGER NOT NULL
    );
    """,
    dag=dag
)

# 4. Load Data Task
load_data = PythonOperator(
    task_id="load_data",
    python_callable=insert_data_from_df,
    dag=dag
)



# Task Dependencies
download_file >> reading_file >> transform_file_step >> create_table >> load_data
