from ETL.transform import transformation
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Insert Data into PostgreSQL
def insert_data_from_df():
    df = transformation()
    pg_hook = PostgresHook(postgres_conn_id='postgres_load')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO student (
            gender, age, city, profession, academic_pressure, work_pressure,
            cgpa, study_satisfaction, job_satisfaction, sleep_duration, dietary_habits,
            degree, suicidal_thoughts, work_study_hours, financial_stress,
            family_history, depression
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    # Batch Insert for Efficiency
    records = [
        (
            row['gender'], row['age'], row['city'], row['profession'], row['academic_pressure'],
            row['work_pressure'], row['cgpa'], row['study_satisfaction'], row['job_satisfaction'],
            row['sleep_duration'], row['dietary_habits'], row['degree'], row['suicidal_thoughts'],
            row['work_study_hours'], row['financial_stress'], row['family_history'], row['depression']
        )
        for _, row in df.iterrows()
    ]

    cursor.executemany(insert_query, records)  # Efficient batch execution
    conn.commit()
    cursor.close()
    conn.close()
    
  