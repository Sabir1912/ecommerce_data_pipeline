import os
import sys
import psycopg2
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def main():
    # Force PySpark to use the same Python interpreter for workers as the driver
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    # 1. DB Configurations from environment
    db_host = os.environ.get('DB_HOST', 'postgres')
    db_port = os.environ.get('DB_PORT', '5432')
    db_name = os.environ.get('DB_NAME', 'ecommerce_dw')
    db_user = os.environ.get('DB_USER', 'postgres')
    db_password = os.environ.get('DB_PASSWORD', 'postgres')

    raw_data_dir = os.environ.get('RAW_DATA_DIR', '/opt/airflow/data/raw')
    reports_dir = os.environ.get('REPORTS_DIR', '/opt/airflow/data/reports')
    os.makedirs(reports_dir, exist_ok=True)

    print("Initializing target PostgreSQL database schemas...")
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw_clean;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
        print("Database schemas 'raw_clean' and 'analytics' created/verified.")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error initializing schemas: {e}")
        sys.exit(1)

    print("Starting Spark Session...")
    # Include PostgreSQL JDBC driver package to connect Spark with Postgres
    spark = SparkSession.builder \
        .appName("EcommerceDataEngineeringPipeline") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Ingest Raw Datasets
    print("Reading raw CSV files...")
    raw_cust = spark.read.csv(os.path.join(raw_data_dir, 'customers.csv'), header=True, inferSchema=True)
    raw_prod = spark.read.csv(os.path.join(raw_data_dir, 'products.csv'), header=True, inferSchema=True)
    raw_orders = spark.read.csv(os.path.join(raw_data_dir, 'orders.csv'), header=True, inferSchema=True)
    raw_order_items = spark.read.csv(os.path.join(raw_data_dir, 'order_items.csv'), header=True, inferSchema=True)
    raw_pay = spark.read.csv(os.path.join(raw_data_dir, 'payments.csv'), header=True, inferSchema=True)

    # 3. Data Quality Checks & Data Cleaning
    print("Executing Data Quality layer...")
    metrics = []

    # --- CUSTOMERS ---
    total_cust = raw_cust.count()
    # Check for empty customer_id or empty name
    null_cust_id = raw_cust.filter(F.col("customer_id").isNull() | (F.col("customer_id") == "")).count()
    null_cust_name = raw_cust.filter(F.col("name").isNull() | (F.col("name") == "")).count()
    # Check for invalid emails (no @ character)
    invalid_emails = raw_cust.filter(~F.col("email").like("%@%")).count()
    # Check duplicate customer_id
    duplicate_cust = raw_cust.groupBy("customer_id").count().filter(F.col("count") > 1).count()

    metrics.append({"table": "customers", "metric": "total_records", "value": total_cust})
    metrics.append({"table": "customers", "metric": "null_customer_ids", "value": null_cust_id})
    metrics.append({"table": "customers", "metric": "null_names", "value": null_cust_name})
    metrics.append({"table": "customers", "metric": "invalid_emails", "value": invalid_emails})
    metrics.append({"table": "customers", "metric": "duplicate_customer_ids", "value": duplicate_cust})

    # Cleaning: remove records with missing ID or name, invalid email, and drop duplicates
    clean_cust = raw_cust \
        .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != "")) \
        .filter(F.col("name").isNotNull() & (F.col("name") != "")) \
        .filter(F.col("email").like("%@%")) \
        .dropDuplicates(["customer_id"])

    # --- PRODUCTS ---
    total_prod = raw_prod.count()
    null_prod_id = raw_prod.filter(F.col("product_id").isNull() | (F.col("product_id") == "")).count()
    negative_prices = raw_prod.filter(F.col("price") < 0).count()
    negative_stock = raw_prod.filter(F.col("stock") < 0).count()
    null_category = raw_prod.filter(F.col("category").isNull() | (F.col("category") == "")).count()

    metrics.append({"table": "products", "metric": "total_records", "value": total_prod})
    metrics.append({"table": "products", "metric": "null_product_ids", "value": null_prod_id})
    metrics.append({"table": "products", "metric": "negative_prices", "value": negative_prices})
    metrics.append({"table": "products", "metric": "negative_stocks", "value": negative_stock})
    metrics.append({"table": "products", "metric": "null_categories", "value": null_category})

    # Cleaning: remove products with null IDs, negative prices, negative stock, and null categories
    clean_prod = raw_prod \
        .filter(F.col("product_id").isNotNull() & (F.col("product_id") != "")) \
        .filter(F.col("price") >= 0) \
        .filter(F.col("stock") >= 0) \
        .filter(F.col("category").isNotNull() & (F.col("category") != ""))

    # --- ORDERS ---
    total_orders = raw_orders.count()
    null_order_id = raw_orders.filter(F.col("order_id").isNull() | (F.col("order_id") == "")).count()
    null_ord_cust_id = raw_orders.filter(F.col("customer_id").isNull() | (F.col("customer_id") == "")).count()
    # Check invalid date formats
    raw_orders_date = raw_orders.withColumn("parsed_date", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
    invalid_dates = raw_orders_date.filter(F.col("parsed_date").isNull()).count()
    # Check future dates
    future_dates = raw_orders_date.filter(F.col("parsed_date") > F.current_date()).count()

    metrics.append({"table": "orders", "metric": "total_records", "value": total_orders})
    metrics.append({"table": "orders", "metric": "null_order_ids", "value": null_order_id})
    metrics.append({"table": "orders", "metric": "null_customer_ids", "value": null_ord_cust_id})
    metrics.append({"table": "orders", "metric": "invalid_dates", "value": invalid_dates})
    metrics.append({"table": "orders", "metric": "future_dates", "value": future_dates})

    # Cleaning: keep only orders with valid order_id, valid customer_id, parseable date, and date in the past
    clean_orders = raw_orders_date \
        .filter(F.col("order_id").isNotNull() & (F.col("order_id") != "")) \
        .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != "")) \
        .filter(F.col("parsed_date").isNotNull()) \
        .filter(F.col("parsed_date") <= F.current_date()) \
        .select("order_id", "customer_id", F.col("parsed_date").alias("order_date"), "status", "total_amount")

    # --- ORDER ITEMS ---
    total_items = raw_order_items.count()
    null_item_id = raw_order_items.filter(F.col("order_item_id").isNull() | (F.col("order_item_id") == "")).count()
    negative_item_prices = raw_order_items.filter(F.col("price") < 0).count()
    negative_item_qtys = raw_order_items.filter(F.col("quantity") <= 0).count()

    metrics.append({"table": "order_items", "metric": "total_records", "value": total_items})
    metrics.append({"table": "order_items", "metric": "null_item_ids", "value": null_item_id})
    metrics.append({"table": "order_items", "metric": "negative_prices", "value": negative_item_prices})
    metrics.append({"table": "order_items", "metric": "negative_or_zero_quantities", "value": negative_item_qtys})

    # Cleaning order items
    clean_order_items = raw_order_items \
        .filter(F.col("order_item_id").isNotNull() & (F.col("order_item_id") != "")) \
        .filter(F.col("price") >= 0) \
        .filter(F.col("quantity") > 0)

    # --- PAYMENTS ---
    total_pay = raw_pay.count()
    null_pay_id = raw_pay.filter(F.col("payment_id").isNull() | (F.col("payment_id") == "")).count()
    null_pay_order_id = raw_pay.filter(F.col("order_id").isNull() | (F.col("order_id") == "")).count()
    negative_payments = raw_pay.filter(F.col("amount") <= 0).count()

    metrics.append({"table": "payments", "metric": "total_records", "value": total_pay})
    metrics.append({"table": "payments", "metric": "null_payment_ids", "value": null_pay_id})
    metrics.append({"table": "payments", "metric": "null_order_ids", "value": null_pay_order_id})
    metrics.append({"table": "payments", "metric": "negative_or_zero_amounts", "value": negative_payments})

    # Cleaning: keep only payments with positive amounts
    clean_pay = raw_pay \
        .filter(F.col("payment_id").isNotNull() & (F.col("payment_id") != "")) \
        .filter(F.col("order_id").isNotNull() & (F.col("order_id") != "")) \
        .filter(F.col("amount") > 0)

    # 4. Generate & Save Data Quality Report
    print("Writing data quality report...")
    dq_df = pd.DataFrame(metrics)
    dq_report_path = os.path.join(reports_dir, 'data_quality_report.csv')
    dq_df.to_csv(dq_report_path, index=False)
    print(f"Data Quality Report saved to {dq_report_path}")

    # Calculate overall DQ Score
    total_errors = sum([m['value'] for m in metrics if m['metric'] != 'total_records'])
    total_rows = sum([m['value'] for m in metrics if m['metric'] == 'total_records'])
    dq_score = round((1.0 - (total_errors / total_rows)) * 100.0, 2) if total_rows > 0 else 100.0
    print(f"Pipeline Data Quality Score: {dq_score}%")

    # 5. Load Clean Raw Tables into PostgreSQL
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    connection_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }

    print("Loading cleaned transactional tables to raw_clean schema...")
    clean_cust.write.jdbc(jdbc_url, "raw_clean.customers", mode="overwrite", properties=connection_properties)
    clean_prod.write.jdbc(jdbc_url, "raw_clean.products", mode="overwrite", properties=connection_properties)
    clean_orders.write.jdbc(jdbc_url, "raw_clean.orders", mode="overwrite", properties=connection_properties)
    clean_order_items.write.jdbc(jdbc_url, "raw_clean.order_items", mode="overwrite", properties=connection_properties)
    clean_pay.write.jdbc(jdbc_url, "raw_clean.payments", mode="overwrite", properties=connection_properties)

    # 6. Advanced Analytics & Transformations
    print("Computing aggregate analytics transformations...")

    # A. Customer Lifetime Value (CLV)
    # Sum payment amounts for completed/non-cancelled orders grouped by customer
    # Joining orders and payments
    cust_payments = clean_orders \
        .filter(F.col("status") != "Cancelled") \
        .join(clean_pay, "order_id", "inner")

    clv_df = cust_payments \
        .groupBy("customer_id") \
        .agg(
            F.round(F.sum("amount"), 2).alias("total_spent"),
            F.countDistinct("order_id").alias("order_count"),
            F.round(F.avg("amount"), 2).alias("avg_order_value")
        )

    # B. Top Customers Ranking
    window_spec = Window.orderBy(F.col("total_spent").desc())
    top_customers_df = clv_df \
        .join(clean_cust.select("customer_id", "name", "country"), "customer_id", "inner") \
        .withColumn("rank", F.rank().over(window_spec)) \
        .select("rank", "customer_id", "name", "country", "total_spent", "order_count", "avg_order_value")

    # C. Monthly Revenue Trend
    monthly_revenue_df = cust_payments \
        .withColumn("order_month", F.date_format(F.col("order_date"), "yyyy-MM")) \
        .groupBy("order_month") \
        .agg(
            F.round(F.sum("amount"), 2).alias("revenue"),
            F.countDistinct("order_id").alias("orders_count")
        ) \
        .orderBy("order_month")

    # D. Product Category Performance
    product_sales = clean_order_items \
        .join(clean_prod.drop("price"), "product_id", "inner") \
        .join(clean_orders.filter(F.col("status") != "Cancelled"), "order_id", "inner")

    product_performance_df = product_sales \
        .groupBy("product_id", "product_name", "category") \
        .agg(
            F.sum("quantity").alias("quantity_sold"),
            F.round(F.sum(F.col("quantity") * F.col("price")), 2).alias("total_revenue")
        ) \
        .orderBy(F.col("total_revenue").desc())

    # E. Pipeline Run Metadata (for Operations Dashboard)
    run_timestamp = datetime.now()
    metadata_records = [
        (run_timestamp.strftime("%Y-%m-%d %H:%M:%S"), "SUCCESS", total_rows, total_errors, dq_score)
    ]
    columns = ["run_time", "status", "total_records_processed", "total_errors_found", "data_quality_score"]
    pipeline_metrics_df = spark.createDataFrame(metadata_records, schema=columns)

    # 7. Load Aggregated Analytics Tables into Postgres
    print("Loading aggregate tables to analytics schema...")
    top_customers_df.write.jdbc(jdbc_url, "analytics.top_customers", mode="overwrite", properties=connection_properties)
    monthly_revenue_df.write.jdbc(jdbc_url, "analytics.monthly_revenue", mode="overwrite", properties=connection_properties)
    product_performance_df.write.jdbc(jdbc_url, "analytics.product_performance", mode="overwrite", properties=connection_properties)
    
    # Append the run metric history
    pipeline_metrics_df.write.jdbc(jdbc_url, "analytics.pipeline_runs", mode="append", properties=connection_properties)

    # Write detailed dq checks for dashboards to query
    dq_records_df = spark.createDataFrame(dq_df)
    dq_records_df.write.jdbc(jdbc_url, "analytics.data_quality_metrics", mode="overwrite", properties=connection_properties)

    print("ETL Job completed successfully!")
    spark.stop()

if __name__ == '__main__':
    main()
