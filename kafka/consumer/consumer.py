from kafka import KafkaConsumer
from json import loads
import psycopg2
import sys

# PostgreSQL configuration
DB_HOST = 'db'
DB_PORT = 5432
DB_NAME = 'salary_db'
DB_USER = 'postgres'
DB_PASS = 'admin'

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
TOPIC_NAME = 'job_salaries'
GROUP_ID = 'your_group_id'

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    conn.autocommit = True
    cursor = conn.cursor()
    print("Connected to PostgreSQL")
except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")
    sys.exit(1)

# Connect to Kafka
try:
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )
    print("Connected to Kafka")
except Exception as e:
    print(f"Error connecting to Kafka: {e}")
    sys.exit(1)

print("Consumer started. Listening for messages...")

for message in consumer:
    data = message.value
    print(f"Received message: {data}")

    # Prepare data for insertion
    try:
        insert_query = """
        INSERT INTO job_salaries (
            work_year,
            experience_level,
            employment_type,
            job_title,
            salary,
            salary_currency,
            salary_in_usd,
            employee_residence,
            remote_ratio,
            company_location,
            company_size
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        # Extract values from data
        work_year = data.get('work_year')
        experience_level = data.get('experience_level')
        employment_type = data.get('employment_type')
        job_title = data.get('job_title')
        salary = data.get('salary')
        salary_currency = data.get('salary_currency')
        salary_in_usd = data.get('salary_in_usd')
        employee_residence = data.get('employee_residence')
        remote_ratio = data.get('remote_ratio')
        company_location = data.get('company_location')
        company_size = data.get('company_size')
        # Add more columns as needed

        cursor.execute(insert_query, (
            work_year,
            experience_level,
            employment_type,
            job_title,
            salary,
            salary_currency,
            salary_in_usd,
            employee_residence,
            remote_ratio,
            company_location,
            company_size
        ))
        print("Data inserted into PostgreSQL")
    except Exception as e:
        print(f"Error inserting data into PostgreSQL: {e}")
        # Optionally, log the error or handle it accordingly
