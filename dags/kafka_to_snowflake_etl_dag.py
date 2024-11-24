from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
#from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyspark.sql import SparkSession
# from pyspark import SparkConf
import jaydebeapi
import pyspark
from pyspark.sql import SparkSession
import pyarrow
import pyarrow.parquet as pq
from pyspark.sql.functions import col
import logging
import pickle
import os
import jpype

from airflow.providers.jdbc.hooks.jdbc import JdbcHook
#from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook
import pandas as pd

from pyspark.sql.functions import avg, mean, count, col, first


def load_data_using_jdbc():
    """
    Load data from PostgreSQL, perform transformations, and create three separate Parquet files:
    1. All data
    2. Prediction data
    3. Transformed data
    """
    # Start JDBC Connection
    if not jpype.isJVMStarted():
        jpype.startJVM(classpath=["/opt/airflow/postgresql-42.5.4.jar"])

    try:
        # Establish JDBC connection using jaydebeapi
        conn = jaydebeapi.connect(
            "org.postgresql.Driver",
            "jdbc:postgresql://db:5432/salary_db",
            ["postgres", "admin"],
            "/opt/airflow/postgresql-42.5.4.jar"
        )
        cursor = conn.cursor()

        # Load All Data
        sql_query = "SELECT * FROM job_salaries;"
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        print(f"Number of rows fetched for all data: {len(rows)}")
        if len(rows) == 0:
            raise ValueError("No data returned from job_salaries table.")
        columns = [
            'work_year', 'experience_level', 'employment_type', 'job_title',
            'salary', 'salary_currency', 'salary_in_usd', 'employee_residence',
            'remote_ratio', 'company_location', 'company_size'
        ]
        pandas_df = pd.DataFrame(rows, columns=columns)
        pandas_df = pandas_df.dropna()
        if pandas_df.empty:
            raise ValueError("pandas_df is empty. Check the job_salaries table or query.")

        # Save All Data as Parquet
        spark = SparkSession.builder \
            .appName("ETL_Transformation") \
            .config("spark.network.timeout", "3000s") \
            .config("spark.executor.heartbeatInterval", "1000s") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        print("Spark session created.")
        
        # Convert Pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(pandas_df)
        
        # Flatten nested fields and cast data types
        flat_spark_df = spark_df.select(
            col("work_year").cast("long"),
            col("experience_level").cast("string").alias("experience_level"),
            col("employment_type").cast("string").alias("employment_type"),
            col("job_title").alias("job_title"),
            col("salary").cast("double"),
            col("salary_currency").alias("salary_currency"),
            col("salary_in_usd").cast("long"),
            col("employee_residence").alias("employee_residence"),
            col("remote_ratio").cast("double"),
            col("company_location").cast("string").alias("company_location"),
            col("company_size").cast("string").alias("company_size")
        )

        print("Checking for null values in all data...")
        flat_spark_df = flat_spark_df.dropna()
        
        # Save All Data as Parquet
        all_data_path = "/opt/airflow/shared/all_data.parquet"
        flat_spark_df.write.mode("overwrite").parquet(all_data_path)
        print(f"All data saved to {all_data_path}")

        # Load Prediction Data
        prediction_query = "SELECT * FROM new_predictions;"
        cursor.execute(prediction_query)
        prediction_rows = cursor.fetchall()
        prediction_columns = [
            'work_year', 'experience_level', 'employment_type', 'job_title',
            'employee_residence', 'remote_ratio', 'company_location',
            'company_size', 'predicted_salary'
        ]
        prediction_df = pd.DataFrame(prediction_rows, columns=prediction_columns)
        if prediction_df.empty:
            raise ValueError("Prediction table is empty. Check the new_predictions table or query.")

        # Convert Pandas DataFrame to Spark DataFrame for prediction data
        prediction_spark_df = spark.createDataFrame(prediction_df)

        # Flatten and handle nulls in prediction data
        prediction_flat_df = prediction_spark_df.select(
            col("work_year").cast("long"),
            col("experience_level").cast("string").alias("experience_level"),
            col("employment_type").cast("string").alias("employment_type"),
            col("job_title").cast("string").alias("job_title"),
            col("remote_ratio").cast("double").alias("remote_ratio"),
            col("employee_residence").cast("string").alias("employee_residence"),
            col("company_location").cast("string").alias("company_location"),
            col("company_size").cast("string").alias("company_size"),
            col("predicted_salary").cast("double").alias("predicted_salary")
        )
        print("Checking for null values in prediction data...")
        prediction_flat_df = prediction_flat_df.dropna()
        
        # Save Prediction Data as Parquet
        prediction_data_path = "/opt/airflow/shared/prediction_data.parquet"
        prediction_flat_df.write.mode("overwrite").parquet(prediction_data_path)
        print(f"Prediction data saved to {prediction_data_path}")

        # Transform Data
        transformed_df = flat_spark_df.groupBy("experience_level", "job_title").agg(
            avg("salary_in_usd").alias("average_salary"),
            mean("remote_ratio").alias("remote_ratio_mean"),
            count("job_title").alias("total_jobs"),
            first("company_size").alias("company_size")
        )

        # Save Transformed Data as Parquet
        transformed_data_path = "/opt/airflow/shared/transformed_data.parquet"
        transformed_df.write.mode("overwrite").parquet(transformed_data_path)

        print(f"Transformed data saved to {transformed_data_path}")

    except Exception as e:
        print("Error during data processing:", e)
    finally:
        cursor.close()
        conn.close()


default_args = {
    'owner': 'Vaibhav Bansal',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    description="A pipeline to read data from Kafka, process with PySpark, and store in Snowflake",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def kafka_to_snowflake_pipeline():

    @task
    def start_kafka_producer():
        try:
            task = DockerOperator(
                task_id="start_kafka_producer",
                image="kafka-producer",
                api_version="auto",
                auto_remove=True,
                command="python producer.py",
                docker_url="unix://var/run/docker.sock",
                network_mode="kafka-net",
            )
            task.execute(context={})
            logging.info("Kafka producer completed successfully.")
            return True
        except Exception as e:
            logging.error(f"Kafka producer failed with error: {e}")
            return False

    @task
    def start_kafka_consumer():
        try:
            task = DockerOperator(
                task_id="start_kafka_consumer",
                image="kafka-consumer",
                api_version="auto",
                auto_remove=True,
                command="python consumer.py",
                docker_url="unix://var/run/docker.sock",
                network_mode="kafka-net",
            )
            task.execute(context={})
            logging.info("Kafka Consumer completed successfully.")
            return True
        except Exception as e:
            logging.error(f"Kafka Consumer failed with error: {e}")
            return False

    jdbc_to_spark_task = PythonOperator(
        task_id='jdbc_to_spark_task',
        python_callable=load_data_using_jdbc,
    )
    
    create_salary_data_table = SnowflakeOperator(
    task_id='create_salary_data_table',
    snowflake_conn_id='snowflake_default',
    sql=""" 
        CREATE OR REPLACE TABLE salary_data_snowflake (
            work_year INT,
            experience_level VARCHAR,
            employment_type VARCHAR,
            job_title VARCHAR,
            salary FLOAT,
            salary_currency VARCHAR,
            salary_in_usd FLOAT,
            employee_residence VARCHAR,
            remote_ratio INT,
            company_location VARCHAR,
            company_size VARCHAR
        );
        """
    )

    create_transformed_salary_data_table = SnowflakeOperator(
        task_id='create_transformed_salary_data_table',
        snowflake_conn_id='snowflake_default',
        sql=""" 
        CREATE OR REPLACE TABLE transformed_salary_data (
            experience_level VARCHAR,
            employment_type VARCHAR,
            job_title VARCHAR,
            average_salary FLOAT,
            remote_ratio_mean FLOAT,
            total_jobs INT,
            company_size VARCHAR
        );
        """
    )

    create_prediction_data_table = SnowflakeOperator(
        task_id='create_prediction_data_table',
        snowflake_conn_id='snowflake_default',
        sql=""" 
        CREATE OR REPLACE TABLE prediction_data (
            work_year INT,
            experience_level VARCHAR,
            employment_type VARCHAR,
            job_title VARCHAR,
            employee_residence VARCHAR,
            company_location VARCHAR,
            predicted_salary FLOAT
        );
        """
    )
    
    parquet_file_path_all = '/opt/airflow/shared/all_data.parquet'
    parquet_file_path_pred = '/opt/airflow/shared/prediction_data.parquet'
    parquet_file_path_transform = '/opt/airflow/shared/transformed_data.parquet'

    try:
        parquet_dir_all = '/opt/airflow/shared/all_data.parquet'
        parquet_files_all = [
            f for f in os.listdir(parquet_dir_all) if f.endswith('.parquet')
        ]

        if not parquet_files_all:
            raise FileNotFoundError("No Parquet files found in the directory.")

        parquet_file_path_all = os.path.join(parquet_dir_all, parquet_files_all[0])
        
        parquet_dir_pred = '/opt/airflow/shared/prediction_data.parquet'
        parquet_files_pred = [
            f for f in os.listdir(parquet_dir_pred) if f.endswith('.parquet')
        ]

        if not parquet_files_pred:
            raise FileNotFoundError("No Parquet files found in the directory.")

        parquet_file_path_pred = os.path.join(parquet_dir_pred, parquet_files_pred[0])
        
        parquet_dir_transform = '/opt/airflow/shared/transformed_data.parquet'
        parquet_files_transform = [
            f for f in os.listdir(parquet_dir_transform) if f.endswith('.parquet')
        ]

        if not parquet_files_transform:
            raise FileNotFoundError("No Parquet files found in the directory.")

        parquet_file_path_transform = os.path.join(parquet_dir_transform, parquet_files_transform[0])
    except Exception as e:
        logging.error(f"Error occurred while fetching parquet files: {e}")

    
    upload_all_data_to_stage = SnowflakeOperator(
        task_id="upload_all_data_to_stage",
        snowflake_conn_id="snowflake_default",
        sql=f""" 
        PUT file://{parquet_file_path_all} @~/stage/ AUTO_COMPRESS=FALSE;
        """
    )

    copy_all_data_to_table = SnowflakeOperator(
        task_id="copy_all_data_to_table",
        snowflake_conn_id="snowflake_default",
        sql=""" 
        COPY INTO salary_job.public.salary_data_snowflake
        FROM @~/stage/
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """
    )


    upload_prediction_data_to_stage = SnowflakeOperator(
        task_id="upload_prediction_data_to_stage",
        snowflake_conn_id="snowflake_default",
        sql=f""" 
        PUT file://{parquet_file_path_pred} @~/stage/ AUTO_COMPRESS=FALSE;
        """
    )

    copy_prediction_data_to_table = SnowflakeOperator(
        task_id="copy_prediction_data_to_table",
        snowflake_conn_id="snowflake_default",
        sql=""" 
        COPY INTO salary_job.public.prediction_data
        FROM @~/stage/
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """
    )


    upload_transformed_data_to_stage = SnowflakeOperator(
        task_id="upload_transformed_data_to_stage",
        snowflake_conn_id="snowflake_default",
        sql=f""" 
        PUT file://{parquet_file_path_transform} @~/stage/ AUTO_COMPRESS=FALSE;
        """
    )

    copy_transformed_data_to_table = SnowflakeOperator(
        task_id="copy_transformed_data_to_table",
        snowflake_conn_id="snowflake_default",
        sql=""" 
        COPY INTO salary_job.public.transformed_salary_data
        FROM @~/stage/
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """
    )


    validate_data = PostgresOperator(
        task_id='validate_data',
        postgres_conn_id='postgres_default',
        sql="SELECT COUNT(*) FROM job_salaries;",
    )
    
   
    @task
    def print_start_message():
        print("Starting the Airflow DAG: kafka_to_snowflake_pipeline")

    print_start_message() >> start_kafka_producer() >> start_kafka_consumer() >> jdbc_to_spark_task >>  create_salary_data_table >> create_transformed_salary_data_table >> create_prediction_data_table >> [
        upload_all_data_to_stage,
        upload_prediction_data_to_stage,
         upload_transformed_data_to_stage,
    ] >> copy_all_data_to_table >>copy_prediction_data_to_table >>copy_transformed_data_to_table >> validate_data

kafka_to_snowflake_pipeline_dag = kafka_to_snowflake_pipeline()
