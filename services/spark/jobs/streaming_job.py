import json
import os
import re
from collections import Counter, defaultdict, deque
from datetime import datetime, timezone, timedelta

from cassandra.cluster import Cluster
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import BooleanType, IntegerType, LongType, StringType, StructField, StructType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "wiki_analytics")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
RAW_BUCKET = os.getenv("RAW_BUCKET", "wiki-raw")
CHECKPOINT_BUCKET = os.getenv("CHECKPOINT_BUCKET", "wiki-checkpoints")

STOP_WORDS = {
    "the", "and", "of", "in", "on", "for", "to", "a", "an", "by", "with", "from",
    "та", "і", "в", "у", "на", "для", "з", "до", "від", "de", "la", "el", "le", "der", "die", "das"
}

DOMAIN_EVENTS = defaultdict(deque)
KEYWORD_EVENTS = defaultdict(deque)
USER_EVENTS = defaultdict(deque)
BOT_EVENTS = defaultdict(deque)
USER_DOMAINS = defaultdict(set)
PREVIOUS_MINUTE_COUNTS = defaultdict(int)
LAST_ALERT_AT = {}

URL_RE = re.compile(r"https?://|www\.", re.IGNORECASE)
PHONE_RE = re.compile(r"(\+?\d[\d\s\-()]{7,}\d)")
TOKEN_RE = re.compile(r"[\wА-Яа-яІіЇїЄєҐґ]+", re.UNICODE)

schema = StructType([
    StructField("event_id", StringType()),
    StructField("domain", StringType()),
    StructField("page_id", LongType()),
    StructField("page_title", StringType()),
    StructField("user_id", LongType()),
    StructField("user_name", StringType()),
    StructField("is_bot", BooleanType()),
    StructField("created_at", StringType()),
    StructField("title_length", IntegerType()),
    StructField("raw", StringType()),
])


def parse_dt(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if not value:
        return datetime.now(timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def get_session():
    cluster = Cluster([CASSANDRA_HOST])
    return cluster.connect(KEYSPACE)


def get_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda v: str(v).encode("utf-8"),
    )


def prune(q, cutoff):
    while q and q[0][0] < cutoff:
        q.popleft()


def tokens(title):
    return [t.lower() for t in TOKEN_RE.findall(title or "") if len(t) > 2 and t.lower() not in STOP_WORDS]


def send_once(producer, topic, key, alert, cooldown_seconds=300):
    now = datetime.now(timezone.utc)
    last = LAST_ALERT_AT.get(key)
    if last and (now - last).total_seconds() < cooldown_seconds:
        return
    LAST_ALERT_AT[key] = now
    producer.send(topic, key=key, value=alert)


def detect_activity_spike(event, producer):
    now = parse_dt(event["created_at"])
    domain = event["domain"]
    q = DOMAIN_EVENTS[domain]
    q.append((now, event["page_title"]))
    prune(q, now - timedelta(hours=1))

    last_5 = [(t, title) for t, title in q if t >= now - timedelta(minutes=5)]
    pages_last_5 = len(last_5)
    avg_per_5 = max(len(q) / 12.0, 0.01)
    ratio = pages_last_5 / avg_per_5

    if pages_last_5 >= 10 and ratio > 3:
        alert = {
            "alert_time": now.isoformat(),
            "alert_type": "activity_spike",
            "domain": domain,
            "pages_last_5min": pages_last_5,
            "avg_pages_per_5min": round(avg_per_5, 2),
            "spike_ratio": round(ratio, 2),
            "sample_pages": [x[1] for x in last_5[-5:]],
        }
        send_once(producer, "breaking-news-alerts", f"spike:{domain}", alert)


def detect_keyword_burst(event, producer):
    now = parse_dt(event["created_at"])
    for token in tokens(event["page_title"]):
        q = KEYWORD_EVENTS[token]
        q.append((now, event["page_id"], event["domain"], event["page_title"]))
        prune(q, now - timedelta(minutes=10))
        pages = {x[1] for x in q}
        if len(pages) >= 5:
            alert = {
                "alert_time": now.isoformat(),
                "alert_type": "keyword_burst",
                "keyword": token,
                "occurrences": len(pages),
                "domains": sorted({x[2] for x in q}),
                "sample_pages": list(dict.fromkeys([x[3] for x in q]))[:5],
            }
            send_once(producer, "breaking-news-alerts", f"keyword:{token}", alert)


def detect_bot_alerts(event, producer):
    now = parse_dt(event["created_at"])
    domain = event["domain"]
    user_name = event["user_name"]
    key = (domain, user_name)
    if event["is_bot"]:
        q = BOT_EVENTS[key]
        q.append((now, event["page_title"]))
        prune(q, now - timedelta(minutes=10))
        if len(q) > 50:
            alert = {
                "alert_time": now.isoformat(),
                "alert_type": "single_bot_overload",
                "domain": domain,
                "bot_name": user_name,
                "pages_last_10min": len(q),
                "sample_pages": [x[1] for x in list(q)[-5:]],
            }
            send_once(producer, "bot-alerts", f"bot-overload:{domain}:{user_name}", alert)


def detect_spam(event, producer):
    now = parse_dt(event["created_at"])
    title = event["page_title"] or ""
    domain = event["domain"]
    user_id = int(event.get("user_id") or 0)
    user_name = event["user_name"]
    reasons = []
    severity = "low"

    if not event["is_bot"]:
        q = USER_EVENTS[user_id]
        q.append((now, domain, title))
        prune(q, now - timedelta(minutes=5))
        if len(q) > 10:
            reasons.append("non_bot_user_created_more_than_10_pages_in_5min")
            severity = "high"

    if URL_RE.search(title):
        reasons.append("title_contains_url")
        severity = "high"
    if PHONE_RE.search(title):
        reasons.append("title_contains_phone_number")
        severity = "medium" if severity == "low" else severity
    digits = sum(ch.isdigit() for ch in title)
    if len(title) >= 8 and digits / max(len(title), 1) > 0.45:
        reasons.append("title_contains_too_many_digits")
        severity = "medium" if severity == "low" else severity
    if len(title) < 3 or len(title) > 120:
        reasons.append("title_length_anomaly")
        severity = "medium" if len(title) > 120 else severity

    USER_DOMAINS[user_id].add(domain)
    if not event["is_bot"] and len(USER_EVENTS[user_id]) <= 3 and len(USER_DOMAINS[user_id]) >= 3:
        reasons.append("new_user_multiple_domains")
        severity = "medium" if severity == "low" else severity

    if reasons:
        alert = {
            "alert_time": now.isoformat(),
            "alert_type": "spam_or_vandalism",
            "severity": severity,
            "domain": domain,
            "user_id": user_id,
            "user_name": user_name,
            "page_id": event["page_id"],
            "page_title": title,
            "reasons": reasons,
        }
        producer.send("spam-alerts", key=f"spam:{user_id}:{event['page_id']}", value=alert)


def write_batch(batch_df, batch_id):
    rows = [r.asDict(recursive=True) for r in batch_df.collect()]
    if not rows:
        return

    session = get_session()
    producer = get_producer()

    insert_domain = session.prepare("""
        INSERT INTO pages_by_domain_time
        (domain, bucket_date, created_at, page_id, page_title, user_id, user_name, is_bot, title_length, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    insert_details = session.prepare("""
        INSERT INTO page_details
        (page_id, domain, page_title, user_id, user_name, is_bot, created_at, title_length, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    insert_user = session.prepare("""
        INSERT INTO pages_by_user
        (user_id, created_at, page_id, domain, page_title, user_name, is_bot)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)
    insert_lang = session.prepare("""
        INSERT INTO language_activity
        (domain, minute_bucket, pages_created, unique_authors, avg_title_length, trend_percent)
        VALUES (?, ?, ?, ?, ?, ?)
    """)
    insert_bot = session.prepare("""
        INSERT INTO bot_activity_metrics
        (domain, minute_bucket, bot_pages, human_pages, bot_percent, top_bots, top_humans)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    minute_groups = defaultdict(list)
    for e in rows:
        if not e.get("domain") or not e.get("page_id"):
            continue
        created = parse_dt(e["created_at"])
        raw_json = json.dumps(e.get("raw") or {}, ensure_ascii=False)
        session.execute(insert_domain, (e["domain"], created.date(), created, int(e["page_id"]), e["page_title"], int(e.get("user_id") or 0), e["user_name"], bool(e["is_bot"]), int(e["title_length"] or 0), raw_json))
        session.execute(insert_details, (int(e["page_id"]), e["domain"], e["page_title"], int(e.get("user_id") or 0), e["user_name"], bool(e["is_bot"]), created, int(e["title_length"] or 0), raw_json))
        session.execute(insert_user, (int(e.get("user_id") or 0), created, int(e["page_id"]), e["domain"], e["page_title"], e["user_name"], bool(e["is_bot"])))

        detect_activity_spike(e, producer)
        detect_keyword_burst(e, producer)
        detect_bot_alerts(e, producer)
        detect_spam(e, producer)

        minute = created.replace(second=0, microsecond=0)
        minute_groups[(e["domain"], minute)].append(e)

    for (domain, minute), events in minute_groups.items():
        pages = len(events)
        authors = {int(x.get("user_id") or 0) for x in events}
        avg_title = sum(int(x.get("title_length") or 0) for x in events) / pages
        previous = PREVIOUS_MINUTE_COUNTS[domain]
        trend = None if previous == 0 else ((pages - previous) / previous) * 100
        PREVIOUS_MINUTE_COUNTS[domain] = pages
        session.execute(insert_lang, (domain, minute, pages, len(authors), float(avg_title), None if trend is None else float(trend)))

        bot_pages = sum(1 for x in events if x.get("is_bot"))
        human_pages = pages - bot_pages
        bot_percent = (bot_pages / pages) * 100 if pages else 0.0
        bot_counter = Counter(x["user_name"] for x in events if x.get("is_bot"))
        human_counter = Counter(x["user_name"] for x in events if not x.get("is_bot"))
        session.execute(insert_bot, (
            domain, minute, bot_pages, human_pages, float(bot_percent),
            json.dumps([{"name": k, "pages": v} for k, v in bot_counter.most_common(5)], ensure_ascii=False),
            json.dumps([{"name": k, "pages": v} for k, v in human_counter.most_common(5)], ensure_ascii=False),
        ))
        if pages >= 5 and bot_percent > 80:
            producer.send("bot-alerts", key=f"bot-percent:{domain}:{minute.isoformat()}", value={
                "alert_time": datetime.now(timezone.utc).isoformat(),
                "alert_type": "high_bot_activity",
                "domain": domain,
                "minute_bucket": minute.isoformat(),
                "bot_percent": round(bot_percent, 2),
                "bot_pages": bot_pages,
                "human_pages": human_pages,
            })

    producer.flush()
    session.shutdown()


def main():
    spark = (
        SparkSession.builder.appName("WikipediaSparkStreamingAnalytics")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", "page-create-events") \
        .option("startingOffsets", "latest") \
        .load()

    parsed = raw.select(from_json(col("value").cast("string"), schema).alias("e")).select("e.*")

    raw_query = parsed.withColumn("raw_json", to_json(struct("*"))).writeStream \
        .format("parquet") \
        .option("path", f"s3a://{RAW_BUCKET}/page-create-events") \
        .option("checkpointLocation", f"s3a://{CHECKPOINT_BUCKET}/raw-writer") \
        .outputMode("append") \
        .start()

    cassandra_query = parsed.writeStream \
        .foreachBatch(write_batch) \
        .option("checkpointLocation", f"s3a://{CHECKPOINT_BUCKET}/streaming-analytics") \
        .start()

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
