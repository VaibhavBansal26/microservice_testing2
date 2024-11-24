FROM python:3.8-slim


WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY kafka/producer/producer.py .
COPY data/processed_data/ds_role_salary.csv /data/processed_data/ds_role_salary.csv

RUN pip install --no-cache-dir kafka-python findspark \
    && for i in 1 2 3; do pip install --default-timeout=1000 --no-cache-dir pyspark && break || sleep 10; done

CMD ["python", "producer.py"]

