FROM spark:3.5.1-python3

USER root

RUN python3 -m pip install --no-cache-dir cassandra-driver boto3 kafka-python && \
    mkdir -p /home/spark/.ivy2/cache && \
    chmod -R 777 /home/spark/.ivy2 && \
    mkdir -p /tmp/spark-events && \
    chmod -R 777 /tmp/spark-events

WORKDIR /app

COPY jobs /app/jobs

USER spark