from kafka import KafkaProducer
import json
import csv
import os
import time
import sys
import logging

# Kafka setup
# KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC_NAME = os.getenv('TOPIC_NAME', 'job_salaries')

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

csv_path = '/data/processed_data/ds_role_salary.csv'

try:
    # Read CSV and send each row to Kafka
    with open(csv_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            producer.send(TOPIC_NAME, row)
            # time.sleep(1)  # Adjust this if needed
            print(f"Sent: {row}")

    producer.flush()
    logging.info("All data sent successfully.")
    sys.exit(0)
except Exception as e:
    print(f"Error in Kafka producer: {e}")
    sys.exit(1)
finally:
    producer.close()

