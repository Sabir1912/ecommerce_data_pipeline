# E-Commerce Data Engineering & Analytics Pipeline

A production-grade, containerized data engineering pipeline designed to ingest raw transactional data, perform distributed quality assurance and cleaning using Apache Spark, load data into a structured PostgreSQL Data Warehouse, and present business intelligence insights through an interactive Streamlit dashboard. The entire workflow is orchestrated seamlessly using Apache Airflow.

---

## 🏗️ Architecture Overview

The system architecture utilizes a modern data stack running inside isolated Docker containers:

```mermaid
graph TD
    A[generate_data.py] -->|1. Generate Raw CSVs| B[(data/raw/)]
    B -->|2. Ingest Data| C[PySpark ETL Job]
    C -->|3. Validate & Clean| D[Data Quality Validation]
    D -->|4a. Generate Report| E[(data/reports/data_quality_report.csv)]
    D -->|4b. Bulk Write Clean Data| F[(PostgreSQL DW)]
    
    subgraph PostgreSQL Warehouse (ecommerce_dw)
        F --> F1[raw_clean Schema]
        F --> F2[analytics Schema]
    end
    
    G[Apache Airflow] -->|Orchestrates Tasks| A
    G -->|Orchestrates Tasks| C
    G -->|Runs Verification Query| F
    
    H[Streamlit Web App] -->|Queries Aggregates & Logs| F
```

1. **Orchestration (Apache Airflow)**: Schedules and coordinates the execution steps of the pipeline, handles task dependencies, and performs end-to-end database validation.
2. **Ingestion & Generation**: A Python module creates mock datasets containing transactional behavior alongside intentionally injected data anomalies to stress-test pipeline durability.
3. **Data Quality & Processing (PySpark)**: Implements validation checks, logs data metrics, calculates an overall Data Quality (DQ) score, quarantines invalid records, and loads clean records into database schemas.
4. **Data Warehouse (PostgreSQL)**: Serves as the central repository with transactional staging tables and calculated analytical reporting views.
5. **Business Intelligence (Streamlit)**: A rich, interactive dashboard mapping KPIs, user retention, product metrics, and pipeline health parameters.

---

## 🛠️ Key Features

* **Anomalous Data Generation**: Ingests fake data with errors such as negative prices, missing primary keys, invalid email formats, future order dates, and exact record duplicates to mirror real-world dirty data.
* **PySpark Distributed Transformation**:
  * Evaluates null constraints, schema formats, validation ranges (prices $\ge 0$, quantities $> 0$), and unique indexes.
  * Calculates an operational **Data Quality Score** per pipeline execution.
  * Writes analytical summaries directly to a local reports volume.
* **Structured Data Warehouse**:
  * `raw_clean` schema: Re-structured staging tables of cleaned, validated transactional records.
  * `analytics` schema: Aggregate metrics ready for high-performance dashboard rendering (monthly revenue, top customer lifetime value, product popularity indices).
* **Operational Telemetry**: Appends real-time execution statistics (`run_time`, `status`, `total_records_processed`, `total_errors_found`, `data_quality_score`) to a persistence table, visible directly from the administration dashboard.
* **Premium User Interface**: Includes interactive 3D visualizations for customer value distributions (CLV vs. purchase frequency), 3D product performance matrices, and an orthographic 3D geographic globe representing global customer locations.

---

## 📂 Project Structure

```
ecommerce_data_pipeline/
├── dags/
│   └── ecommerce_dag.py        # Airflow DAG defining scheduling and operational flows
├── scripts/
│   ├── generate_data.py       # Custom data generation utility
│   └── spark_etl.py           # PySpark data validation, cleaning, and DW load script
├── streamlit_app/
│   ├── app.py                 # Streamlit dashboard interface
│   └── requirements.txt       # Dashboard python dependencies
├── postgres-init/
│   └── init-db.sh             # Custom database initialization script for PostgreSQL container
├── data/                      # Shared storage folder for CSV inputs and reports (generated at runtime)
│   ├── raw/
│   └── reports/
├── Dockerfile                 # Custom Airflow image with Java, PySpark, and PostgreSQL drivers
└── docker-compose.yml         # Core multi-container service declaration
```

---

## 🚀 Getting Started

### Prerequisites

* [Docker](https://www.docker.com/get-started) and [Docker Compose](https://docs.docker.com/compose/install/) installed on your machine.
* At least 4GB of RAM allocated to the Docker Engine (required for PySpark and Airflow services).

### Step-by-Step Installation

1. Navigate to the project directory:
   ```bash
   cd C:/Users/sksab/.gemini/antigravity/scratch/ecommerce_data_pipeline
   ```

2. Spin up the containerized services:
   ```bash
   docker-compose up -d --build
   ```
   *This command will build the custom Airflow base image (including Java 17 and PySpark dependencies), initialize PostgreSQL, create the `airflow` and `ecommerce_dw` databases, and launch the Streamlit frontend.*

3. Monitor container startup status:
   ```bash
   docker-compose ps
   ```

---

## 🔗 Port Mappings & Services

Once the containers are running, you can access the various services using the credentials below:

| Service | Port | Local URL | Credentials / Details |
| :--- | :---: | :--- | :--- |
| **Apache Airflow** | `8080` | [http://localhost:8080](http://localhost:8080) | Username: `admin` <br> Password: `admin` |
| **Streamlit Dashboard** | `8501` | [http://localhost:8501](http://localhost:8501) | Live visual reporting engine |
| **PostgreSQL Database** | `5432` | `localhost:5432` | Username: `postgres` <br> Password: `postgres` <br> Target Database: `ecommerce_dw` |

---

## 🏃 Running the Pipeline

1. Open the **Airflow Webserver** at [http://localhost:8080](http://localhost:8080).
2. Log in using `admin` / `admin`.
3. Locate the DAG named **`ecommerce_pipeline_dag`**.
4. Unpause the DAG (click the toggle switch on the left).
5. Trigger the DAG manually by clicking the **Play** button on the top right.
6. Airflow will execute the tasks in order:
   * **`generate_raw_data`**: Generates source CSVs under `data/raw/`.
   * **`run_spark_etl`**: Starts Spark, cleans the datasets, saves the validation CSV to `data/reports/`, and populates PostgreSQL.
   * **`validate_warehouse`**: Asserts the schema load by querying record counts.
7. Navigate to the **Streamlit Dashboard** at [http://localhost:8501](http://localhost:8501) to explore the populated charts, pipeline logs, and analytics!

---

## 📊 Database Schema Details

The pipeline loads two primary schemas inside the `ecommerce_dw` database:

### 1. `raw_clean` (Clean Staging Warehouse)
* **`customers`**: Unique identifiers, customer names, validated emails, signup dates, and countries.
* **`products`**: Cleaned product catalogs with non-negative prices/stock and classified categories.
* **`orders`**: Orders with valid customer IDs, validated historic dates, total amounts, and fulfillment status.
* **`order_items`**: Line items per order matching products and quantity purchases.
* **`payments`**: Payment transaction status mapping successful billing events.

### 2. `analytics` (Business Intelligence Layer)
* **`top_customers`**: Aggregated lifetime spend, purchase frequency, and average order values (used for VIP segmentation).
* **`monthly_revenue`**: Month-by-month revenue growth trends and transaction counts.
* **`product_performance`**: Detailed breakdown of items sold, category distribution, and gross product revenue.
* **`pipeline_runs`**: Historic telemetry of pipeline executions storing record counts, isolated errors, and performance health metrics.
* **`data_quality_metrics`**: Individual test checks tracking isolated validation failures per table.
