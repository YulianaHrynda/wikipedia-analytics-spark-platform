import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "wiki_analytics")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
RAW_BUCKET = os.getenv("RAW_BUCKET", "wiki-raw")


def cassandra_session():
    cluster = Cluster([CASSANDRA_HOST])
    return cluster.connect(KEYSPACE)


def floor_hour(dt):
    return dt.replace(minute=0, second=0, microsecond=0)


def full_hours_window(hours=6):
    now = datetime.now(timezone.utc)
    end = floor_hour(now)
    start = end - timedelta(hours=hours)
    return start, end


def load_recent_pages(session, hours=24):
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=hours)
    domains = set()
    for row in session.execute("SELECT domain, minute_bucket FROM language_activity"):
        if row.minute_bucket and row.minute_bucket.replace(tzinfo=timezone.utc) >= since:
            domains.add(row.domain)

    rows = []
    for domain in domains:
        for day_offset in range(2):
            bucket = (now - timedelta(days=day_offset)).date()
            query = """
                SELECT domain, created_at, page_id, page_title, user_id, user_name, is_bot, title_length
                FROM pages_by_domain_time
                WHERE domain=%s AND bucket_date=%s
            """
            for row in session.execute(query, (domain, bucket)):
                created = row.created_at.replace(tzinfo=timezone.utc)
                if created >= since:
                    rows.append(row)
    return rows


def build_hourly_reports(session, rows):
    start, end = full_hours_window(6)
    grouped = defaultdict(list)
    for row in rows:
        created = row.created_at.replace(tzinfo=timezone.utc)
        if start <= created < end:
            grouped[(row.domain, floor_hour(created))].append(row)

    insert_report = session.prepare("""
        INSERT INTO hourly_activity_reports
        (domain, hour_start, hour_end, pages_created, unique_authors, bot_percent, top_authors, categories_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)
    insert_summary = session.prepare("""
        INSERT INTO domain_hourly_summary
        (domain, hour_start, pages_created, unique_authors, bot_percent)
        VALUES (?, ?, ?, ?, ?)
    """)

    for (domain, hour), events in grouped.items():
        pages = len(events)
        authors = {int(e.user_id or 0) for e in events}
        bots = sum(1 for e in events if e.is_bot)
        bot_percent = (bots / pages) * 100 if pages else 0.0
        by_author = Counter((e.user_name, bool(e.is_bot)) for e in events)
        top_authors = [
            {"name": name, "pages": count, "is_bot": is_bot}
            for (name, is_bot), count in by_author.most_common(10)
        ]
        session.execute(insert_report, (
            domain, hour, hour + timedelta(hours=1), pages, len(authors), float(bot_percent),
            json.dumps(top_authors, ensure_ascii=False), "{}"
        ))
        session.execute(insert_summary, (domain, hour, pages, len(authors), float(bot_percent)))


def build_editor_patterns(session, rows, min_pages=5):
    by_user = defaultdict(list)
    for row in rows:
        by_user[int(row.user_id or 0)].append(row)

    insert_pattern = session.prepare("""
        INSERT INTO editor_behavior_patterns
        (user_id, user_name, total_pages, avg_seconds_between_pages, active_hours, domains_count,
         dominant_domain, dominant_domain_percent, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    for user_id, events in by_user.items():
        if user_id == 0 or len(events) <= min_pages:
            continue
        events = sorted(events, key=lambda e: e.created_at)
        diffs = []
        for prev, cur in zip(events, events[1:]):
            diffs.append((cur.created_at - prev.created_at).total_seconds())
        avg_gap = sum(diffs) / len(diffs) if diffs else None
        hours = Counter(e.created_at.hour for e in events)
        domains = Counter(e.domain for e in events)
        dominant_domain, dominant_count = domains.most_common(1)[0]
        dominant_pct = dominant_count / len(events) * 100
        active_hours = [{"hour_utc": h, "pages": c} for h, c in sorted(hours.items())]
        session.execute(insert_pattern, (
            user_id,
            events[-1].user_name,
            len(events),
            None if avg_gap is None else float(avg_gap),
            json.dumps(active_hours, ensure_ascii=False),
            len(domains),
            dominant_domain,
            float(dominant_pct),
            datetime.now(timezone.utc),
        ))


def main():
    # Spark is used here as the batch runtime and to verify MinIO raw archive availability.
    spark = (
        SparkSession.builder.appName("WikipediaSparkBatchAnalytics")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw_df = spark.read.parquet(f"s3a://{RAW_BUCKET}/page-create-events")
        print(f"Raw archive rows in MinIO: {raw_df.count()}")
    except Exception as exc:
        print(f"Raw archive is not ready yet: {exc}")

    session = cassandra_session()
    rows = load_recent_pages(session, hours=24)
    print(f"Loaded {len(rows)} recent Cassandra page rows for batch analytics")
    build_hourly_reports(session, rows)
    build_editor_patterns(session, rows)
    session.shutdown()
    spark.stop()


if __name__ == "__main__":
    main()
