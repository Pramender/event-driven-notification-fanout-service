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

| Requirement | Notes |
|-------------|-------|
| **Docker & Docker Compose** | Required for the full stack (`docker compose up --build`) and for integration tests (Testcontainers) |
| **Java 21** | Matches `pom.xml` target; needed for local `mvn` runs |
| **Maven 3.9+** | Build and test runner |
| **jq** (optional) | Used by `manual-curls.sh` for pretty-printing responses |

Integration tests spin up **PostgreSQL** and **Kafka** containers automatically. Docker must be running when executing `mvn test`.

### Run with Docker Compose (full stack)

```bash
docker compose up --build
```

This starts PostgreSQL, Kafka, Jaeger, and the fanout service with tracing enabled.

| Service | URL |
|---------|-----|
| Fanout API | http://localhost:8080 |
| Swagger UI | http://localhost:8080/swagger-ui.html |
| Prometheus metrics | http://localhost:8080/actuator/prometheus |
| Jaeger UI | http://localhost:16686 |

### Local development (dependencies only)

Run infrastructure in Docker and the service on the host for faster iteration:

```bash
# Start dependencies only
docker compose up postgres kafka jaeger -d

# Run service (defaults connect to localhost)
export DB_HOST=localhost
export KAFKA_BOOTSTRAP=localhost:9092
export MANAGEMENT_TRACING_ENABLED=true
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318/v1/traces
mvn spring-boot:run
```

### Manual API smoke test

Sample JSON payloads and a scripted walkthrough live under `src/test/resources/api-samples/`:

```bash
export WEBHOOK_BASE="https://webhook.site/your-unique-id"   # or a local WireMock URL
./src/test/resources/api-samples/manual-curls.sh
```

The script creates three sample subscriptions, posts events from `api-samples/events/`, waits for the delivery worker, queries audit endpoints, then **replays** historical events for consumer 1 (`order-alerts`, `AT_LEAST_ONCE`) and re-checks audit for a second delivery attempt. Subscription JSON files use `{{WEBHOOK_URL}}` placeholders replaced at runtime.

Replay example (replace `{subscriptionId}`; window must include sample `occurred_at` timestamps):

```bash
curl -s -X POST "http://localhost:8080/v1/subscriptions/{subscriptionId}/replay" \
  -H 'Content-Type: application/json' \
  -d '{"from": "2026-06-27T00:00:00Z", "to": "2026-06-27T23:59:59Z"}' | jq '{eventsScanned, matched, queued, skipped}'
```

Replay scans persisted events by effective timestamp (`occurred_at`, or `received_at` if absent), re-evaluates the subscription filter, and enqueues or requeues deliveries according to status and `deliveryMode`.

| Sample | Purpose |
|--------|---------|
| `subscriptions/consumer-1-order-alerts.json` | `order.created` with `payload.amount >= 100` |
| `subscriptions/consumer-2-payment-hooks.json` | `payment.completed` events |
| `subscriptions/consumer-3-inventory-alerts.json` | `inventory.low_stock` events |
| `events/order-created-high-value.json` | Matches consumer 1 (amount 150) |
| `events/order-created-low-value.json` | Does not match consumer 1 (amount 50) |
| `events/payment-completed.json` | Matches consumer 2 |
| `events/inventory-low-stock.json` | Matches consumer 3 |
| `events/invalid-missing-payload.json` | Validation failure example |

### Run tests

```bash
mvn test          # unit + integration tests
mvn verify        # same as CI (includes packaging checks)
```

**Unit tests** cover filter evaluation, delivery state machine, retry backoff, event ingest validation, replay logic, webhook target validation, structured logging, and exception-to-HTTP mapping.

**Integration tests** use Testcontainers (PostgreSQL, Kafka) and WireMock for webhook simulation. See `flow_diagram.md` → [Test coverage — scenarios](flow_diagram.md#test-coverage--scenarios) for the full list.

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

Filters are JSON objects evaluated by `FilterEvaluator` against each inbound event. A **null or missing filter matches everything**.

### Composition

Top-level (and nested) groups use **`all`** (AND) or **`any`** (OR). Leaf nodes are `{ "field", "op", "value" }` conditions.

**Match high-value orders from billing or payments:**

```json
{
  "version": 1,
  "all": [
    { "field": "type", "op": "eq", "value": "order.created" },
    { "field": "payload.amount", "op": "gte", "value": 100 },
    {
      "any": [
        { "field": "source", "op": "eq", "value": "billing-service" },
        { "field": "source", "op": "in", "value": ["payments-api", "checkout"] }
      ]
    }
  ]
}
```

**Match several event types (OR at top level):**

```json
{
  "any": [
    { "field": "type", "op": "eq", "value": "order.created" },
    { "field": "type", "op": "eq", "value": "payment.completed" }
  ]
}
```

**Nested AND inside OR:**

```json
{
  "any": [
    {
      "all": [
        { "field": "type", "op": "eq", "value": "inventory.low_stock" },
        { "field": "payload.warehouse", "op": "exists" }
      ]
    },
    { "field": "type", "op": "eq", "value": "inventory.critical" }
  ]
}
```

### Fields

| Field pattern | Resolves to |
|---------------|-------------|
| `type` | Event type string |
| `source` | Event source string |
| `payload.<path>` | Dot-path into JSON payload (e.g. `payload.order.customer.tier`) |

Unknown top-level fields (not `type`, `source`, or `payload.*`) cause a filter evaluation error at fanout time.

### Operators

| Operator | Applies to | Semantics |
|----------|------------|-----------|
| `eq` | All fields | Equal (numbers compared as doubles; strings as text) |
| `neq` | All fields | Not equal |
| `in` | All fields | Actual value is in `value` array |
| `not_in` | All fields | Actual value is not in `value` array |
| `exists` | All fields | Field is present and non-null |
| `contains` | Text fields (typically `payload.*`) | Actual string contains expected substring |
| `gte`, `gt`, `lte`, `lt` | Numeric fields (typically `payload.*`) | Numeric comparison; both sides must be numbers |

**Examples by operator:**

```json
{ "field": "type", "op": "neq", "value": "debug.ping" }
{ "field": "source", "op": "not_in", "value": ["test-harness", "load-gen"] }
{ "field": "payload.sku", "op": "exists" }
{ "field": "payload.message", "op": "contains", "value": "CRITICAL" }
{ "field": "payload.amount", "op": "lt", "value": 50 }
```

Unsupported operators (e.g. `regex`) or invalid conditions throw `FilterEvaluationException`, which surfaces as **400 Bad Request** on event ingest when fanout evaluates the subscription filter.

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
| POST | `/v1/events` | Accept event (HTTP ingress; same pipeline as Kafka) |
| POST | `/v1/subscriptions` | Create subscription |
| GET | `/v1/subscriptions` | List active subscriptions |
| GET | `/v1/subscriptions/{id}` | Get subscription |
| DELETE | `/v1/subscriptions/{id}` | Soft-delete subscription |
| POST | `/v1/subscriptions/{id}/replay` | Re-enqueue historical events in a time window |
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

Structured logs with consistent fields: `timestamp`, `action`, `status`, plus correlation IDs (`traceId`, `eventId`, `subscriptionId`, `deliveryId`, `sequenceNumber`).

- **Local dev (default):** readable single-line console format with short stack traces on errors
- **Production (`SPRING_PROFILES_ACTIVE=prod`):** JSON logs via Logstash encoder for log aggregation

Example (dev console):

```
2026-06-27 10:14:32.123 ERROR action=delivery.dispatch status=FAILED event= delivery=abc-123 attempt=3 httpStatus=503 reason=HTTP 503 | Delivery permanently failed
```

## Simplicity tradeoffs vs production hardening

This service prioritizes a small, understandable codebase over enterprise-grade resilience. The table below reflects **actual implementation choices** and what would typically be hardened next.

| Area | Current (simple) behavior | Production hardening direction |
|------|---------------------------|--------------------------------|
| **Subscription cache** | In-process TTL cache (`SUBSCRIPTION_CACHE_TTL`, default 30s). Invalidated on create/delete, but not on every read. | Push invalidation (pub/sub), shorter TTL with metrics on staleness, or read-through without cache for fanout-critical paths |
| **Fanout matching** | Full scan of all active subscriptions per event with in-process filter evaluation | Indexed routing (by `type`, bloom filters, or precompiled rules) to avoid O(subscriptions) per event |
| **Fanout in ingest transaction** | Event persist + fanout + delivery enqueue share one DB transaction | Async fanout via outbox table + worker; caps ingest latency under high subscription count |
| **Delivery transport** | `@Scheduled` poller reads `subscription_deliveries` (batch size 20); `deliveries.pending` Kafka topic is configured but **unused** | Transactional outbox → Kafka/SQS per delivery; dedicated worker pool with backpressure |
| **Stuck `IN_FLIGHT`** | Scheduler only selects `QUEUED` and `RETRY_PENDING` at head-of-line. Crash after marking `IN_FLIGHT` leaves row stuck | Reaper job: reset stale `IN_FLIGHT` after timeout; or lease-based dispatch with heartbeat |
| **Webhook client** | New `RestClient` per attempt; no circuit breaker, bulkhead, or rate limit per subscriber | Shared HTTP client pool, per-subscription circuit breaker, adaptive timeouts |
| **Scheduler scaling** | Single JVM `@Scheduled` loop; row locking uses `FOR UPDATE SKIP LOCKED` for concurrency within one instance | Leader-elected scheduler or partition by `subscription_id`; horizontal workers with consistent hashing |
| **FIFO head-of-line blocking** | Correct per subscription, but one slow/failing head blocks all later events for that consumer | Optional parallel lanes, priority queues, or dead-letter head after N attempts |
| **Soft delete** | Deleted subscriptions drop out of cache (eventually); existing `QUEUED` deliveries are **not** cancelled | Cancel pending deliveries on delete; or drain with explicit policy |
| **Replay** | Full table scan of events in time window; re-evaluates filter per event | Paginated/cursor-based replay, idempotency tokens, rate limits |
| **Exactly-once webhook** | `AT_MOST_ONCE` skips if `SENT` exists, but 2xx + crash before status update can still redeliver | Subscriber idempotency + transactional webhook ack; or external dedup store |
| **Observability gaps** | Metrics and traces exist; no alerting rules or SLO dashboards in-repo | Alert on DLQ rate, delivery lag, stuck `IN_FLIGHT` count, cache age |

These tradeoffs are documented in more detail in `flow_diagram.md` (delivery guarantees, cache vs DB, and “What is NOT guaranteed”).

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `8080` | HTTP port |
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `fanout` | Database name |
| `DB_USER` | `fanout` | Database user |
| `DB_PASSWORD` | `fanout` | Database password |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka brokers |
| `KAFKA_INBOUND_TOPIC` | `events.inbound` | Event ingress topic |
| `KAFKA_DLQ_TOPIC` | `events.inbound.dlq` | Invalid event dead-letter topic |
| `KAFKA_DELIVERY_TOPIC` | `deliveries.pending` | Configured but **not used** (delivery is DB-polled) |
| `DELIVERY_WORKER_POLL_MS` | `1000` | Delivery scheduler poll interval (ms) |
| `DELIVERY_SCHEDULER_INTERVAL_MS` | `5000` | Legacy/alternate scheduler property in config (worker poll drives dispatch) |
| `SUBSCRIPTION_CACHE_TTL` | `30` | Subscription cache TTL (seconds) |
| `MANAGEMENT_TRACING_ENABLED` | `false` | Enable OpenTelemetry tracing |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318/v1/traces` | OTLP trace exporter URL |
| `SPRING_PROFILES_ACTIVE` | _(unset)_ | Set to `prod` for JSON log encoding |

## CI/CD

GitHub Actions workflow (`.github/workflows/ci.yml`) runs `mvn verify` on every push and pull request.

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
