# Design Document: Wikipedia Analytics Platform

## Goal

The platform detects real-time Wikipedia activity patterns and provides historical analytics for journalists, researchers and moderators.

## Technology choice

- Kafka buffers Wikimedia page-create events and decouples producers from consumers.
- Spark Structured Streaming implements real-time processing with micro-batches and sliding-window style state.
- Spark Batch implements historical analytics over accumulated data.
- Cassandra stores query-optimized tables for REST API access.
- MinIO stores raw normalized events as Parquet, giving the project a data lake layer for replay and offline analysis.
- FastAPI exposes the required ad-hoc and report APIs.

## Data flow

```text
Wikimedia EventStreams API
        |
        v
Python Ingestion Service
        |
        v
Kafka: page-create-events
        |
        v
Spark Structured Streaming
   |             |             |
   v             v             v
MinIO raw     Cassandra      Kafka alert topics
Parquet       metrics/API    breaking/bot/spam
        |
        v
Spark Batch Jobs
        |
        v
Cassandra report tables
        |
        v
FastAPI REST API
```

## Real-time analytics

### A1 Breaking News Detector

Implemented in `services/spark/jobs/streaming_job.py`.

- Activity spike: per-domain one-hour state, current five-minute count compared to average five-minute count.
- Keyword burst: tokenizes page titles, ignores stop words and alerts when a token appears in five or more distinct pages within ten minutes.

Alerts are written to Kafka topic `breaking-news-alerts`.

### A2 Bot vs Human Monitor

The Spark streaming job aggregates each micro-batch by domain and minute, computes bot pages, human pages, bot percentage and top users. Results are written to Cassandra table `bot_activity_metrics`.

Alerts are written to `bot-alerts` when bot percentage is high or one bot creates more than 50 pages within ten minutes.

### A3 Language Activity Dashboard

For every domain and minute the job computes:

- pages created;
- unique authors;
- average title length;
- trend vs previous minute.

Results are written to `language_activity`.

### A4 Spam and Vandalism Detector

The streaming job checks:

- non-bot user creates more than 10 pages in 5 minutes;
- title contains URL;
- title contains phone-like pattern;
- title contains too many digits;
- title is abnormally short or long;
- new user creates pages in several domains.

Alerts are written to Kafka topic `spam-alerts`.

## Batch analytics

Implemented in `services/spark/jobs/batch_job.py`.

### B1 Hourly Activity Report

The job builds reports for completed hours and writes them to `hourly_activity_reports`. The API excludes the current incomplete hour.

### B2 Editor Behavior Patterns

The job computes editor-level behavior for users with more than `min_pages` pages:

- average seconds between page creations;
- active UTC hours;
- number of domains;
- dominant domain and percentage.

Results are written to `editor_behavior_patterns`.

## Cassandra model

Cassandra uses denormalized tables optimized for access patterns:

- `pages_by_domain_time` for domain/time queries and batch scans;
- `page_details` for lookup by page ID;
- `pages_by_user` for lookup by user ID;
- `language_activity` for dashboard metrics;
- `bot_activity_metrics` for bot monitoring;
- `hourly_activity_reports` for B1;
- `editor_behavior_patterns` for B2.

DDL is located in `ddl/cassandra_schema.cql`.

## MinIO model

MinIO acts as an S3-compatible data lake:

- bucket `wiki-raw` stores raw normalized event archive in Parquet;
- bucket `wiki-checkpoints` stores Spark Structured Streaming checkpoints.

This allows replay, offline exploration and separation between raw history and Cassandra query tables.
