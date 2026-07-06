# Order Flow Pipeline

A small event-driven pipeline that simulates e-commerce orders, streams them through Kafka, processes and aggregates them in real time, and exposes the results through an API and Grafana dashboards.

```
generator --> Kafka ("orders" topic) --> processor --> Postgres --> api --> Grafana
```

## Components

| Service     | Description                                                                                   |
|-------------|-----------------------------------------------------------------------------------------------|
| `generator` | Produces synthetic order events to the Kafka `orders` topic at a configurable rate.            |
| `processor` | Consumes orders, persists them, flags high-value orders as alerts, and aggregates per-minute metrics and per-category stats. |
| `api`       | FastAPI service exposing order metrics, alerts, and category stats from Postgres.              |
| `db`        | Postgres schema (`orders`, `order_metrics`, `alerts`, `category_stats`).                       |
| `grafana`   | Pre-provisioned Postgres datasource for building dashboards on top of the API's data.          |

## Prerequisites

- Docker and Docker Compose

## Running

```bash
docker compose up --build
```

This starts Zookeeper, Kafka, Postgres, the generator, the processor, the API, and Grafana.

| Service   | URL                              |
|-----------|-----------------------------------|
| API       | http://localhost:8000             |
| Postgres  | localhost:5432 (`user`/`password`, db `orders`) |
| Grafana   | http://localhost:3000 (`admin`/`admin`) |
| Kafka     | localhost:9092                    |

## API Endpoints

- `GET /health` — liveness check
- `GET /metrics/summary` — total orders, GMV, and average order value over the last hour
- `GET /metrics/timeseries?minutes=30` — per-minute order counts and GMV
- `GET /alerts/recent?limit=20` — most recent high-value order alerts
- `GET /categories/top?region=PL` — top categories by GMV, optionally filtered by region

## Configuration

Set via environment variables in `docker-compose.yml`:

| Variable            | Service    | Default        | Description                        |
|---------------------|------------|----------------|-------------------------------------|
| `EVENTS_PER_SECOND`  | generator  | `2`            | Rate of synthetic order generation  |
| `KAFKA_BROKER`       | generator, processor | `kafka:29092` | Kafka bootstrap server |
| `DATABASE_URL`       | processor, api | postgresql://user:password@postgres:5432/orders | Postgres connection string |

The high-value order alert threshold (`ALERT_THRESHOLD`) is set in `processor/processor.py` (default `$2000`).
