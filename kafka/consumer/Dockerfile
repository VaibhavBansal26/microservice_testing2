FROM python:3.8-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev libpq-dev python3-pip && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install --no-cache-dir kafka-python psycopg2-binary findspark --default-timeout=1000 && \
    for i in 1 2 3; do pip3 install --no-cache-dir pyspark && break || sleep 10; done

COPY kafka/consumer/consumer.py .

CMD ["python", "consumer.py"]