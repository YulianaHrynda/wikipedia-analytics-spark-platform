import json
import os
from datetime import datetime, timezone, timedelta
import time
from cassandra.cluster import Cluster, NoHostAvailable
from dateutil import parser as date_parser


CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "wiki_analytics")


def connect_with_retry():
    last_error = None

    for attempt in range(60):
        try:
            print(f"Connecting to Cassandra attempt {attempt + 1}/60...")
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect(KEYSPACE)
            print("Connected to Cassandra")
            return session
        except Exception as e:
            last_error = e
            print(f"Cassandra not ready yet: {e}")
            time.sleep(3)

    raise last_error


session = connect_with_retry()


def parse_time(value: str) -> datetime:
    return date_parser.parse(value).astimezone(timezone.utc)


def row_to_page(row):
    return {
        "page_id": row.page_id,
        "domain": row.domain,
        "page_title": row.page_title,
        "user_id": row.user_id,
        "user_name": row.user_name,
        "is_bot": row.is_bot,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "title_length": getattr(row, "title_length", None),
    }


def get_domains():
    since = datetime.now(timezone.utc) - timedelta(hours=1)
    rows = session.execute("SELECT domain, minute_bucket, pages_created, unique_authors FROM language_activity")
    stats = {}
    for row in rows:
        if not row.minute_bucket or row.minute_bucket.replace(tzinfo=timezone.utc) < since:
            continue
        item = stats.setdefault(row.domain, {"domain": row.domain, "pages_last_hour": 0, "unique_authors_last_hour": 0})
        item["pages_last_hour"] += row.pages_created or 0
        item["unique_authors_last_hour"] += row.unique_authors or 0

    bot_rows = session.execute("SELECT domain, minute_bucket, bot_pages, human_pages FROM bot_activity_metrics")
    bot_totals = {}
    for row in bot_rows:
        if not row.minute_bucket or row.minute_bucket.replace(tzinfo=timezone.utc) < since:
            continue
        b, h = bot_totals.get(row.domain, (0, 0))
        bot_totals[row.domain] = (b + (row.bot_pages or 0), h + (row.human_pages or 0))

    result = []
    for domain, item in stats.items():
        bots, humans = bot_totals.get(domain, (0, 0))
        total = bots + humans
        item["bot_percent_last_hour"] = round(bots / total * 100, 2) if total else 0.0
        result.append(item)
    return sorted(result, key=lambda x: x["pages_last_hour"], reverse=True)


def get_user_pages(user_id: int, limit: int):
    rows = session.execute(
        "SELECT user_id, created_at, page_id, domain, page_title, user_name, is_bot FROM pages_by_user WHERE user_id=%s LIMIT %s",
        (user_id, limit),
    )
    return [row_to_page(row) for row in rows]


def get_page(page_id: int):
    row = session.execute("SELECT * FROM page_details WHERE page_id=%s", (page_id,)).one()
    if not row:
        return None
    result = row_to_page(row)
    result["raw"] = json.loads(row.raw_json) if getattr(row, "raw_json", None) else None
    return result


def get_domain_pages(domain: str, from_ts: str, to_ts: str, limit: int):
    start = parse_time(from_ts)
    end = parse_time(to_ts)
    results = []
    day = start.date()
    while day <= end.date() and len(results) < limit:
        rows = session.execute(
            "SELECT * FROM pages_by_domain_time WHERE domain=%s AND bucket_date=%s",
            (domain, day),
        )
        for row in rows:
            created = row.created_at.replace(tzinfo=timezone.utc)
            if start <= created <= end:
                results.append(row_to_page(row))
                if len(results) >= limit:
                    break
        day = day + timedelta(days=1)
    return results


def get_hourly_report(domain: str, hours: int):
    now = datetime.now(timezone.utc)
    end = now.replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=hours)
    rows = session.execute("SELECT * FROM hourly_activity_reports WHERE domain=%s", (domain,))
    result = []
    for row in rows:
        hs = row.hour_start.replace(tzinfo=timezone.utc)
        if start <= hs < end:
            result.append({
                "time_start": row.hour_start.isoformat(),
                "time_end": row.hour_end.isoformat(),
                "domain": row.domain,
                "pages_created": row.pages_created,
                "unique_authors": row.unique_authors,
                "bot_percent": row.bot_percent,
                "top_authors": json.loads(row.top_authors or "[]"),
                "categories_count": json.loads(row.categories_count or "{}"),
            })
    return sorted(result, key=lambda x: x["time_start"])


def get_editor_patterns(min_pages: int):
    rows = session.execute("SELECT * FROM editor_behavior_patterns")
    result = []
    for row in rows:
        if row.total_pages and row.total_pages > min_pages:
            result.append({
                "user_id": row.user_id,
                "user_name": row.user_name,
                "total_pages": row.total_pages,
                "avg_seconds_between_pages": row.avg_seconds_between_pages,
                "active_hours": json.loads(row.active_hours or "[]"),
                "domains_count": row.domains_count,
                "dominant_domain": row.dominant_domain,
                "dominant_domain_percent": row.dominant_domain_percent,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
            })
    return sorted(result, key=lambda x: x["total_pages"], reverse=True)
