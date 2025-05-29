# load_to_snowflake.py

from snowflake.connector import connect
from config import snowflake_conn_config

def connect_to_snowflake():
    conn = connect(**snowflake_conn_config)
    print("✅ Connected to Snowflake")

    # Set the writable database and schema
    cursor = conn.cursor()
    cursor.execute("USE DATABASE MY_DEV_DB")   # Your new writable DB
    cursor.execute("USE SCHEMA PUBLIC")
    cursor.close()

    return conn

def create_sample_table(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE OR REPLACE TABLE trip_summary (
                pickup_hour TIMESTAMP,
                avg_fare FLOAT,
                trip_count INT
            )
        """)
        print("✅ Table created successfully.")
    finally:
        cursor.close()

def insert_sample_data(conn):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO trip_summary (pickup_hour, avg_fare, trip_count)
            VALUES 
            ('2025-05-28 08:00:00', 14.5, 120),
            ('2025-05-28 09:00:00', 13.2, 98)
        """)
        print("✅ Sample data inserted.")
    finally:
        cursor.close()

if __name__ == "__main__":
    conn = connect_to_snowflake()
    create_sample_table(conn)
    insert_sample_data(conn)
    conn.close()
