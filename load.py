import psycopg2
from psycopg2.extras import execute_values

# Define the connect function
def connect_postgresql(config):
    try:
        with psycopg2.connect(**config) as conn:
            print("Connected to postgresql Server")
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)
        
def load_data_to_postgresql(conn, df, table_name):
    added_rows = f"""
    INSERT INTO {table_name} ({', '.join(df.columns)})
    VALUES %s
    """
    rowsVal_as_tuple = list(df.itertuples(index = False, name = None))
    try:
        with conn.cursor() as cur:
            execute_values(cur, added_rows, rowsVal_as_tuple)
            conn.commit()
            print(f"Successfully inserted {len(rowsVal_as_tuple)} records into {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"Error inserting data: {e}")
    


