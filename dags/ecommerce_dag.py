from datetime import datetime, timedelta
import os
import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def verify_warehouse():
    """Simple verification task to log row counts from PostgreSQL."""
    db_host = os.environ.get('DB_HOST', 'postgres')
    db_port = os.environ.get('DB_PORT', '5432')
    db_name = os.environ.get('DB_NAME', 'ecommerce_dw')
    db_user = os.environ.get('DB_USER', 'postgres')
    db_password = os.environ.get('DB_PASSWORD', 'postgres')

    print("Connecting to PostgreSQL to verify loaded records...")
    
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )
    cur = conn.cursor()
    
    # Query transactional clean tables
    tables = [
        "raw_clean.customers",
        "raw_clean.products",
        "raw_clean.orders",
        "raw_clean.order_items",
        "raw_clean.payments"
    ]
    
    print("\n--- TRANSACTIONAL TABLES (raw_clean) ---")
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        count = cur.fetchone()[0]
        print(f"Table '{table}': {count} clean records loaded.")

    # Query analytics aggregated tables
    analytics_tables = [
        "analytics.top_customers",
        "analytics.monthly_revenue",
        "analytics.product_performance",
        "analytics.pipeline_runs"
    ]
    
    print("\n--- ANALYTICS TABLES (analytics) ---")
    for table in analytics_tables:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        count = cur.fetchone()[0]
        print(f"Table '{table}': {count} aggregate records computed.")
        
    cur.close()
    conn.close()
    print("\nWarehouse verification complete!")

with DAG(
    'ecommerce_pipeline_dag',
    default_args=default_args,
    description='Orchestrates raw CSV generation, PySpark cleaning/ETL, and loading into PostgreSQL',
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. Generate Raw Data CSVs
    generate_env = os.environ.copy()
    generate_env['DATA_DIR'] = '/opt/airflow/data/raw'
    
    generate_raw_data = BashOperator(
        task_id='generate_raw_data',
        bash_command='python /opt/airflow/scripts/generate_data.py',
        env=generate_env
    )

    # 2. Run PySpark ETL Pipeline
    spark_env = os.environ.copy()
    spark_env.update({
        'DB_HOST': 'postgres',
        'DB_PORT': '5432',
        'DB_NAME': 'ecommerce_dw',
        'DB_USER': 'postgres',
        'DB_PASSWORD': 'postgres',
        'RAW_DATA_DIR': '/opt/airflow/data/raw',
        'REPORTS_DIR': '/opt/airflow/data/reports'
    })

    run_spark_etl = BashOperator(
        task_id='run_spark_etl',
        bash_command='python /opt/airflow/scripts/spark_etl.py',
        env=spark_env
    )

    # 3. Verify Database Load
    validate_warehouse = PythonOperator(
        task_id='validate_warehouse',
        python_callable=verify_warehouse,
    )

    # Define execution order
    generate_raw_data >> run_spark_etl >> validate_warehouse
