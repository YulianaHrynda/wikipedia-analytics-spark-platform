CREATE KEYSPACE IF NOT EXISTS wiki_analytics
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE wiki_analytics;

CREATE TABLE IF NOT EXISTS pages_by_domain_time (
    domain text,
    bucket_date date,
    created_at timestamp,
    page_id bigint,
    page_title text,
    user_id bigint,
    user_name text,
    is_bot boolean,
    title_length int,
    raw_json text,
    PRIMARY KEY ((domain, bucket_date), created_at, page_id)
) WITH CLUSTERING ORDER BY (created_at DESC);

CREATE TABLE IF NOT EXISTS page_details (
    page_id bigint PRIMARY KEY,
    domain text,
    page_title text,
    user_id bigint,
    user_name text,
    is_bot boolean,
    created_at timestamp,
    title_length int,
    raw_json text
);

CREATE TABLE IF NOT EXISTS pages_by_user (
    user_id bigint,
    created_at timestamp,
    page_id bigint,
    domain text,
    page_title text,
    user_name text,
    is_bot boolean,
    PRIMARY KEY ((user_id), created_at, page_id)
) WITH CLUSTERING ORDER BY (created_at DESC);

CREATE TABLE IF NOT EXISTS language_activity (
    domain text,
    minute_bucket timestamp,
    pages_created int,
    unique_authors int,
    avg_title_length double,
    trend_percent double,
    PRIMARY KEY ((domain), minute_bucket)
) WITH CLUSTERING ORDER BY (minute_bucket DESC);

CREATE TABLE IF NOT EXISTS bot_activity_metrics (
    domain text,
    minute_bucket timestamp,
    bot_pages int,
    human_pages int,
    bot_percent double,
    top_bots text,
    top_humans text,
    PRIMARY KEY ((domain), minute_bucket)
) WITH CLUSTERING ORDER BY (minute_bucket DESC);

CREATE TABLE IF NOT EXISTS hourly_activity_reports (
    domain text,
    hour_start timestamp,
    hour_end timestamp,
    pages_created int,
    unique_authors int,
    bot_percent double,
    top_authors text,
    categories_count text,
    PRIMARY KEY ((domain), hour_start)
) WITH CLUSTERING ORDER BY (hour_start DESC);

CREATE TABLE IF NOT EXISTS editor_behavior_patterns (
    user_id bigint PRIMARY KEY,
    user_name text,
    total_pages int,
    avg_seconds_between_pages double,
    active_hours text,
    domains_count int,
    dominant_domain text,
    dominant_domain_percent double,
    updated_at timestamp
);

CREATE TABLE IF NOT EXISTS domain_hourly_summary (
    domain text,
    hour_start timestamp,
    pages_created int,
    unique_authors int,
    bot_percent double,
    PRIMARY KEY ((domain), hour_start)
) WITH CLUSTERING ORDER BY (hour_start DESC);
