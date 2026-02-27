from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os

DB_CONFIG = {
    "host": "postgres",
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

DATA_PATH = "/opt/airflow/data/raw"

def get_last_loaded_month():

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    

    cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.orders (
            order_id VARCHAR,
            customer_id VARCHAR,
            order_status VARCHAR,
            order_purchase_timestamp TIMESTAMP,
            order_month VARCHAR,
            loaded_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    
    cur.execute("SELECT MAX(order_month) FROM raw.orders")
    result = cur.fetchone()[0]
    conn.close()
    return result if result else "2016-08"

def load_orders_incremental(**context):
    last_month = get_last_loaded_month()
    

    df = pd.read_csv(f"{DATA_PATH}/olist_orders_dataset.csv")
    

    df["order_month"] = pd.to_datetime(
        df["order_purchase_timestamp"]
    ).dt.to_period("M").astype(str)

    months_available = sorted(df["order_month"].unique())
    next_months = [m for m in months_available if m > last_month]
    
    if not next_months:
        print("All months already loaded.")
        return
    
    next_month = next_months[0]
    df_month = df[df["order_month"] == next_month]
    
    print(f"Loading month: {next_month} — {len(df_month)} orders")
    

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.orders (
            order_id VARCHAR,
            customer_id VARCHAR,
            order_status VARCHAR,
            order_purchase_timestamp TIMESTAMP,
            order_month VARCHAR,
            loaded_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
 
    cur.execute("DELETE FROM raw.orders WHERE order_month = %s", (next_month,))
    
    for _, row in df_month.iterrows():
        cur.execute("""
            INSERT INTO raw.orders 
            (order_id, customer_id, order_status, order_purchase_timestamp, order_month)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            row["order_id"],
            row["customer_id"],
            row["order_status"],
            row["order_purchase_timestamp"],
            row["order_month"]
        ))
    
    conn.commit()
    conn.close()
    print(f"Month {next_month} loaded successfully.")

with DAG(
    dag_id="ingest_olist_orders",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None, 
    catchup=False,
    tags=["ingestion", "olist"],
) as dag:

    ingest_task = PythonOperator(
        task_id="load_orders_incremental",
        python_callable=load_orders_incremental,
    )