FROM apache/airflow:2.5.0-python3.8

USER root
RUN apt-get update && \
    apt-get install -y \
    build-essential\
    gcc \
    g++ \
    python3-dev \
    openjdk-11-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN curl -o /opt/airflow/postgresql-42.5.4.jar https://jdbc.postgresql.org/download/postgresql-42.5.4.jar
ENV CLASSPATH=/opt/airflow/postgresql-42.5.4.jar
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64

USER airflow
# RUN chown -R airflow:airflow /opt/airflow
RUN pip install --no-cache-dir --timeout=6000 apache-airflow[celery] apache-airflow-providers-apache-spark apache-airflow-providers-snowflake apache-airflow-providers-docker apache-airflow-providers-postgres apache-airflow-providers-jdbc pyspark
