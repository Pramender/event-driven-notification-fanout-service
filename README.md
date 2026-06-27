# event-driven-notification-fanout-service

Event-driven notification fanout service that accepts events from Kafka, matches subscriber filter rules, delivers webhooks in **FIFO order per subscription**, and exposes audit APIs for delivery history.

## Features

- **Kafka ingress** with manual offset commit after durable persistence (accepted events are not lost)
- **Subscription CRUD** with hot cache reload (no restart required)
- **JSON filter DSL** on `type`, `source`, and `payload.*` fields
- **Per-subscription delivery tracking** via `subscription_deliveries` (query failures by subscription or event)
- **Strict FIFO ordering** per subscription with head-of-line blocking on retries
- **Retry policy** with exponential backoff + jitter; typed retryable vs permanent HTTP failures
- **Audit API** with attempt timeline (timestamp, HTTP status, error reason, final status)
- **Observability**: Micrometer metrics (Prometheus), OpenTelemetry tracing, structured JSON logging

## Architecture

```
Kafka (events.inbound) → Ingest → PostgreSQL (events)
                              ↓
                         Fanout (filter match)
                              ↓
                    subscription_deliveries (QUEUED)
                              ↓
              Delivery scheduler (FIFO head-of-line)
                              ↓
                      Webhook HTTP POST
                              ↓
              delivery_attempts (audit) + metrics/traces
```

## Quick start

### Prerequisites

- Docker & Docker Compose
- Java 21 + Maven (for local development without Docker)

### Run with Docker Compose

```bash
docker compose up --build
```

Services:

| Service | URL |
|---------|-----|
| Fanout API | http://localhost:8080 |
| Swagger UI | http://localhost:8080/swagger-ui.html |
| Prometheus metrics | http://localhost:8080/actuator/prometheus |
| Jaeger UI | http://localhost:16686 |

### Create a subscription

```bash
curl -s -X POST http://localhost:8080/v1/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "order-alerts",
    "deliveryMode": "AT_LEAST_ONCE",
    "filter": {
      "version": 1,
      "all": [
        { "field": "type", "op": "eq", "value": "order.created" },
        { "field": "payload.amount", "op": "gte", "value": 100 }
      ]
    },
    "target": {
      "url": "https://webhook.site/your-id",
      "headers": {},
      "timeoutMs": 5000
    },
    "retryPolicy": {
      "maxAttempts": 5,
      "initialBackoffMs": 1000,
      "maxBackoffMs": 60000,
      "multiplier": 2.0
    }
  }'
```

### Publish an event to Kafka

```bash
docker compose exec kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic events.inbound <<<'{"type":"order.created","source":"orders-api","payload":{"order_id":"42","amount":150}}'
```

### Query audit history

```bash
# By event ID
curl http://localhost:8080/v1/audit/events/{eventId}

# Failed deliveries for a subscription
curl 'http://localhost:8080/v1/audit/subscriptions/{subscriptionId}?status=FAILED'
```

## Event schema (Kafka / JSON)

```json
{
  "event_id": "optional-uuid",
  "type": "order.created",
  "source": "orders-api",
  "occurred_at": "2026-06-27T10:00:00Z",
  "payload": { "order_id": "42", "amount": 99.99 }
}
```

- `type`, `source`, and `payload` (object) are required
- `event_id` is auto-generated if omitted
- Duplicate `event_id` ingest is idempotent

## Filter rule syntax (v1)

Top-level composition:

```json
{ "all": [ ...conditions ] }
{ "any": [ ...conditions ] }
```

Conditions:

| Field | Ops |
|-------|-----|
| `type`, `source` | `eq`, `neq`, `in`, `not_in` |
| `payload.path` | `eq`, `neq`, `exists`, `contains`, `gte`, `gt`, `lte`, `lt` |

Nested `all` / `any` groups are supported.

## Delivery semantics

| Mode | Behavior |
|------|----------|
| `AT_LEAST_ONCE` | Retries until success or max attempts; duplicates possible |
| `AT_MOST_ONCE` | Skips if a prior `SENT` exists for the same event + subscription |

Webhook requests include:

- Header `X-Idempotency-Key: {eventId}:{subscriptionId}:{attemptNumber}`
- JSON envelope with `event_id`, `type`, `source`, `payload`, `subscription_id`, `delivery_id`, `delivery_attempt`

### Retry behavior

| HTTP result | Action |
|-------------|--------|
| 2xx | `SENT` (terminal) |
| 5xx, 429 | Retry with backoff |
| Most 4xx | `FAILED` (terminal, no retry) |
| Timeout / connection error | Retry with backoff |

Backoff: `random(0, min(initial * multiplier^(attempt-1), maxBackoff))`

## FIFO ordering

Each subscription maintains:

- `next_assign_seq` — sequence assigned when events are matched
- `next_deliverable_seq` — head-of-line pointer for dispatch

Only the delivery at the head sequence is dispatched. If it enters `RETRY_PENDING`, later sequences wait until the head reaches `SENT` or `FAILED`.

## Delivery states

```
QUEUED → IN_FLIGHT → SENT
                   → RETRY_PENDING → IN_FLIGHT → ...
                   → FAILED
```

## API reference

| Method | Path | Description |
|--------|------|-------------|
| POST | `/v1/subscriptions` | Create subscription |
| GET | `/v1/subscriptions` | List active subscriptions |
| GET | `/v1/subscriptions/{id}` | Get subscription |
| DELETE | `/v1/subscriptions/{id}` | Soft-delete subscription |
| GET | `/v1/audit/events/{eventId}` | Delivery audit by event |
| GET | `/v1/audit/subscriptions/{id}` | Delivery audit by subscription |
| GET | `/v1/audit/deliveries/{deliveryId}` | Single delivery timeline |

## Observability

### Metrics (Micrometer / Prometheus)

- `events.accepted.total`, `events.invalid.total`
- `fanout.matches.total`
- `deliveries.attempts.total`, `deliveries.success.total`, `deliveries.failed.total`
- `delivery.latency` timer

### Tracing

OpenTelemetry spans: `event.accept`, `fanout.evaluate`, `subscription.match`, `delivery.http`, `audit.query.*`

Configure OTLP endpoint:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318/v1/traces
```

### Logging

Structured JSON logs (Logstash encoder) with MDC fields: `traceId`, `eventId`, `subscriptionId`, `deliveryId`, `sequenceNumber`.

## Local development

```bash
# Start dependencies only
docker compose up postgres kafka jaeger -d

# Run service
export DB_HOST=localhost KAFKA_BOOTSTRAP=localhost:9092
mvn spring-boot:run
```

### Run tests

```bash
mvn test
```

Unit tests cover filter evaluation, state machine, and retry policy. Integration tests use Testcontainers (PostgreSQL, Kafka) and WireMock for webhook simulation.

## CI/CD

GitHub Actions workflow (`.github/workflows/ci.yml`) runs `mvn verify` on every push and pull request.

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka brokers |
| `KAFKA_INBOUND_TOPIC` | `events.inbound` | Ingress topic |
| `DELIVERY_WORKER_POLL_MS` | `1000` | Delivery scheduler interval |
| `SUBSCRIPTION_CACHE_TTL` | `30` | Subscription cache TTL (seconds) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318/v1/traces` | Trace exporter |

## Project structure

```
src/main/java/com/eventdriven/notification/fanout/
├── application/     # Use cases (ingest, fanout, delivery, audit)
├── domain/          # Core models and enums
├── infrastructure/  # Kafka, JPA, HTTP, REST adapters
└── config/          # Spring configuration
```

## License

MIT
