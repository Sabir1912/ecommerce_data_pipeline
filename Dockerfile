FROM apache/airflow:2.7.2-python3.10

USER root

# Install OpenJDK 17 (headless) and procps (required by Spark)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       openjdk-17-jre-headless \
       procps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Set Java Home environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install stable PySpark (uses pre-built wheel) and PostgreSQL driver
RUN pip install --no-cache-dir pyspark==3.5.1 psycopg2-binary
