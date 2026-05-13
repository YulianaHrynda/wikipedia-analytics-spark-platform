# Wikipedia Analytics Platform

Data engineering course project for real-time and historical analytics over Wikimedia page-create events.

## Stack

- **Kafka** — event broker for Wikimedia page creation events and alerts.
- **Spark Structured Streaming** — real-time analytics A1-A4.
- **Spark Batch** — historical reports B1-B2.
- **Cassandra** — query-optimized tables for API and dashboards.
- **MinIO** — S3-compatible data lake for raw Parquet event archive and Spark checkpoints.
- **FastAPI** — REST API for ad-hoc queries and reports.

## Architecture

```text
Wikimedia EventStreams
        ↓
Ingestion Service
        ↓
Kafka topic: page-create-events
        ↓
Spark Structured Streaming
   ↙          ↓          ↘
MinIO      Cassandra     Kafka alert topics
raw lake   query tables  breaking/bot/spam alerts
        ↓
Spark Batch Jobs
        ↓
Cassandra report tables
        ↓
FastAPI REST API
```

## Services

| Service | Purpose |
|---|---|
| `kafka` | Kafka broker in KRaft mode |
| `kafka-ui` | UI for Kafka topics |
| `cassandra` | Storage for query tables |
| `minio` | Raw data lake and checkpoints |
| `init` | Creates Kafka topics, Cassandra schema and MinIO buckets |
| `ingestion` | Reads Wikimedia SSE and writes normalized events to Kafka |
| `spark-master` / `spark-worker` | Spark cluster |
| `spark-streaming` | Structured Streaming job for A1-A4 |
| `spark-batch` | Periodic batch job for B1-B2 |
| `api` | FastAPI REST API |

## Run

```bash
docker compose up --build
```

Useful UIs:

- FastAPI docs: <http://localhost:8000/docs>
- Kafka UI: <http://localhost:8080>
- Spark Master UI: <http://localhost:8081>
- MinIO Console: <http://localhost:9001>
  - user: `minioadmin`
  - password: `minioadmin`

## API

```http
GET /api/domains
GET /api/users/{user_id}/pages?limit=100
GET /api/pages/{page_id}
GET /api/domains/{domain}/pages?from=<timestamp>&to=<timestamp>&limit=100
GET /api/reports/hourly?domain=uk.wikipedia.org&hours=6
GET /api/analytics/editor-patterns?min_pages=5
```

## What is stored where?

### Kafka

- `page-create-events` — normalized events from Wikimedia.
- `breaking-news-alerts` — activity spike and keyword burst alerts.
- `bot-alerts` — high bot activity alerts.
- `spam-alerts` — spam/vandalism alerts.

### MinIO

- `s3a://wiki-raw/page-create-events` — raw normalized events in Parquet.
- `s3a://wiki-checkpoints/...` — Spark checkpoints.

### Cassandra

- `pages_by_domain_time`
- `page_details`
- `pages_by_user`
- `language_activity`
- `bot_activity_metrics`
- `hourly_activity_reports`
- `editor_behavior_patterns`

## Notes

Hourly reports follow the rule “last N full hours excluding the current hour”. For example, if the request arrives at 18:35 and `hours=6`, the report covers 12:00-18:00.
