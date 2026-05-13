import time
import boto3
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from cassandra.cluster import Cluster


KAFKA_BOOTSTRAP = "kafka:9092"
CASSANDRA_HOST = "cassandra"
MINIO_ENDPOINT = "http://minio:9000"

KAFKA_TOPICS = [
    "page-create-events",
    "breaking-news-alerts",
    "bot-alerts",
    "spam-alerts",
]

MINIO_BUCKETS = [
    "wiki-raw",
    "wiki-lake",
    "wiki-checkpoints",
]


def wait_for_kafka():
    last_error = None

    for attempt in range(60):
        try:
            print(f"Waiting for Kafka... attempt {attempt + 1}/60")
            admin = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                client_id="wiki-init",
            )
            admin.list_topics()
            print("Kafka is ready")
            return admin
        except Exception as e:
            last_error = e
            print(f"Kafka is not ready yet: {e}")
            time.sleep(2)

    raise RuntimeError(f"Kafka is not ready: {last_error}")


def create_kafka_topics(admin):
    existing_topics = set(admin.list_topics())

    topics_to_create = [
        NewTopic(name=topic, num_partitions=1, replication_factor=1)
        for topic in KAFKA_TOPICS
        if topic not in existing_topics
    ]

    if not topics_to_create:
        print("Kafka topics already exist")
        return

    try:
        admin.create_topics(topics_to_create)
        print(f"Created Kafka topics: {[t.name for t in topics_to_create]}")
    except TopicAlreadyExistsError:
        print("Some Kafka topics already exist")
    except Exception as e:
        print(f"Kafka topic creation warning: {e}")


def wait_for_cassandra():
    last_error = None

    for attempt in range(90):
        try:
            print(f"Waiting for Cassandra... attempt {attempt + 1}/90")
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect()
            session.execute("SELECT release_version FROM system.local")
            print("Cassandra is ready")
            return session
        except Exception as e:
            last_error = e
            print(f"Cassandra is not ready yet: {e}")
            time.sleep(3)

    raise RuntimeError(f"Cassandra is not ready: {last_error}")


def init_cassandra(session):
    ddl_path = "/app/ddl/cassandra_schema.cql"

    print(f"Loading Cassandra schema from {ddl_path}")

    with open(ddl_path, "r", encoding="utf-8") as f:
        cql = f.read()

    statements = []

    current = []
    for line in cql.splitlines():
        stripped = line.strip()

        if not stripped:
            continue

        if stripped.startswith("--"):
            continue

        current.append(line)

        if stripped.endswith(";"):
            statement = "\n".join(current).strip().rstrip(";")
            if statement:
                statements.append(statement)
            current = []

    if current:
        statement = "\n".join(current).strip().rstrip(";")
        if statement:
            statements.append(statement)

    if not statements:
        raise RuntimeError("ddl/cassandra_schema.cql is empty or contains no CQL statements")

    for statement in statements:
        print(f"Executing CQL:\n{statement[:300]}...\n")
        session.execute(statement)

    print("Cassandra schema initialized")


def wait_for_minio_client():
    last_error = None

    for attempt in range(60):
        try:
            print(f"Waiting for MinIO... attempt {attempt + 1}/60")

            s3 = boto3.client(
                "s3",
                endpoint_url=MINIO_ENDPOINT,
                aws_access_key_id="minioadmin",
                aws_secret_access_key="minioadmin",
            )

            s3.list_buckets()
            print("MinIO is ready")
            return s3
        except Exception as e:
            last_error = e
            print(f"MinIO is not ready yet: {e}")
            time.sleep(2)

    raise RuntimeError(f"MinIO is not ready: {last_error}")


def init_minio(s3):
    existing_buckets = {
        bucket["Name"]
        for bucket in s3.list_buckets().get("Buckets", [])
    }

    for bucket in MINIO_BUCKETS:
        if bucket not in existing_buckets:
            s3.create_bucket(Bucket=bucket)
            print(f"Created MinIO bucket: {bucket}")
        else:
            print(f"MinIO bucket already exists: {bucket}")

    print("MinIO buckets initialized")


def main():
    print("Initializing infrastructure...")

    kafka_admin = wait_for_kafka()
    create_kafka_topics(kafka_admin)

    cassandra_session = wait_for_cassandra()
    init_cassandra(cassandra_session)

    minio_client = wait_for_minio_client()
    init_minio(minio_client)

    print("Infrastructure initialized successfully")


if __name__ == "__main__":
    main()