# Gopay

> A production-grade Go microservices payment platform — order management, payment processing, and immutable audit trail, connected via Kafka and observable end-to-end.

---

## Overview

Gopay is a backend fintech platform that simulates a real-world payment and order management system. Built entirely in **Go 1.23**, it is composed of three independent services that communicate asynchronously through Kafka events, each owning its own PostgreSQL database. All interaction happens via REST APIs documented with OpenAPI/Swagger.

The architecture is built around four signature engineering patterns that go well beyond standard CRUD:

| Pattern | Description |
|---|---|
| **Outbox Cursor Protocol** | Zero-event-loss Kafka delivery with Redis-backed keyset pagination — no full table scans |
| **Implicit Idempotency via Request Fingerprinting** | SHA-256 content fingerprinting deduplicates requests without requiring client headers |
| **Event Contract Registry** | JSON Schema validation at consumption — schema violations go to DLQ, never silently corrupt data |
| **Payment Capability Projection** | CQRS read model that computes per-customer risk tiers from payment history |

---

## Architecture

```
┌─────────────────┐        order.created         ┌──────────────────┐
│                 │ ──────────────────────────►  │                  │
│  order-service  │                               │ payment-service  │
│   :8081         │ ◄──────────────────────────  │   :8082          │
│                 │    payment.settled / .failed  │                  │
└────────┬────────┘                               └────────┬─────────┘
         │                                                 │
         │           order.created                         │  payment.settled
         │           payment.settled         ┌─────────────┘  payment.failed
         │           payment.failed          │
         │                          ┌────────▼─────────┐
         └─────────────────────────►│  audit-service   │
                                    │   :8083          │
                                    └──────────────────┘

All events flow through Redpanda (Kafka-compatible).
Each service has its own PostgreSQL database.
Redis is shared for idempotency, rate limiting, and outbox cursor.
```

### Canonical Event Flow

1. Client sends `POST /orders` to order-service
2. order-service writes an `orders` row and an `outbox_events` row **in one transaction**
3. `OutboxRelay` goroutine reads from the outbox via cursor query, publishes `order.created` to Kafka, advances the Redis cursor
4. payment-service consumer pool picks up `order.created`, validates it against the contract registry, fingerprints the payload, creates a payment record, processes it, and writes `payment.settled` or `payment.failed` to its own outbox
5. payment-service `OutboxRelay` publishes the payment result to Kafka
6. order-service consumes the result and transitions the order to `confirmed` or `cancelled`
7. audit-service consumes all events and writes immutable audit rows

Every step of this flow is visible in structured logs, Prometheus metrics, and OpenTelemetry traces.

---

## Services

### order-service `:8081`
Owns the order lifecycle. Exposes REST endpoints for creating and querying orders. Drives the saga by publishing `order.created` through the transactional outbox. Consumes payment results to finalize order status.

**Order state machine:**
```
pending → confirmed   (on payment.settled)
pending → cancelled   (on payment.failed)
confirmed → [terminal]
cancelled → [terminal]
```

### payment-service `:8082`
Processes payments triggered by `order.created` events. Maintains the fingerprint idempotency cache and the `payment_capabilities` projection. Publishes payment outcomes back to Kafka.

**Payment state machine:**
```
pending → processing  (on consume order.created)
processing → completed  (on successful processing)
processing → failed   (on processing error after retries)
completed → [terminal]
failed    → [terminal]
```

### audit-service `:8083`
Consumes all events from all topics. Writes immutable `audit_events` rows. Exposes a paginated `GET /audit/events` endpoint (admin role only). Never mutates data from other services.

---

## The Four Signature Patterns

### 1 — Outbox Cursor Protocol
`pkg/outbox/relay.go`

Standard outbox polls with `WHERE published = false`, which becomes a full table scan at volume. The cursor protocol stores the last successfully published event ID in Redis. The relay query is:

```sql
SELECT * FROM outbox_events WHERE id > $1 AND published = FALSE ORDER BY id LIMIT 100
```

The cursor advances on each successful publish. On crash, the cursor survives in Redis and the relay resumes exactly where it stopped — no reprocessing, no event loss. This turns every poll into an O(1) index seek.

Redis key: `outbox:cursor:{service_name}` | Poll interval: 500ms

### 2 — Implicit Idempotency via Request Fingerprinting
`pkg/fingerprint/fingerprinter.go` · `pkg/middleware/idempotency.go`

Standard idempotency requires the client to send an `Idempotency-Key` header — which is impossible when the "client" is a Kafka consumer. Instead, the middleware:

1. Extracts business-identity fields from the payload: `customer_id`, `order_id`, `amount`, `currency`
2. Sorts field names, concatenates values, and SHA-256 hashes the result
3. Checks Redis: `GET fingerprint:{hash}`
4. If found → returns the cached response immediately (200)
5. If not found → processes the request, caches the response with a 24h TTL

Duplicate requests from network retries are deduplicated automatically. The fingerprint is stored on the payment row for full auditability.

### 3 — Event Contract Registry
`pkg/contracts/registry.go` · `pkg/contracts/validator.go`

On startup, each service calls `contracts.Register()` to upsert its event schemas (JSON Schema documents) into the `event_contracts` table. Every Kafka consumer wraps its handler with `contracts.Validate(eventType, version, payload)`.

If a payload fails validation, the message is routed to `audit.dlq` with reason code `SCHEMA_VIOLATION` and the consumer moves on without processing. Producers and consumers can be deployed independently; schema changes are versioned (`order.created.v1`, `order.created.v2`) and both versions coexist.

Schema violations are loud, early, and auditable — never silent.

### 4 — Payment Capability Projection
`payment-service/internal/projection/capability.go`

A dedicated projection worker subscribes to `payment.settled` and `payment.failed` (under a separate consumer group) and maintains the `payment_capabilities` read model per customer:

| Field | Description |
|---|---|
| `total_payments` | Lifetime payment count |
| `successful_payments` | Count of settled payments |
| `success_rate` | Rolling percentage |
| `risk_tier` | `LOW` (≥90%), `MEDIUM` (≥70%), `HIGH` (<70%) |
| `last_payment_at` | Timestamp of most recent payment |
| `total_spent` | Cumulative INR amount |

When `POST /payments` is called, the usecase reads this projection first. If `risk_tier` is `HIGH`, the payment is flagged before processing. This is CQRS in practice: the write path (state machine) is fully separate from the read path (risk assessment).

---

## Tech Stack

| Concern | Choice |
|---|---|
| Language | Go 1.23 |
| HTTP router | Gin |
| ORM | GORM |
| Migrations | golang-migrate (SQL files) |
| PostgreSQL driver | pgx/v5 via GORM pgx adapter |
| Redis | go-redis v9 |
| Kafka | segmentio/kafka-go |
| Local Kafka | Redpanda (no ZooKeeper) |
| Circuit breaker | sony/gobreaker |
| Auth | golang-jwt/jwt v5 + PASETO |
| Metrics | prometheus/client_golang |
| Tracing | go.opentelemetry.io/otel → Tempo |
| Linting | golangci-lint |
| CI | GitHub Actions |

---

## Infrastructure

All services and dependencies run via `docker-compose`:

| Service | Port(s) |
|---|---|
| postgres-order | 5432 |
| postgres-payment | 5433 |
| postgres-audit | 5434 |
| redis | 6379 |
| redpanda | 9092 (Kafka), 8080 (admin console) |
| order-service | 8081 |
| payment-service | 8082 |
| audit-service | 8083 |
| prometheus | 9090 |
| grafana | 3000 |
| tempo | 4317 (OTLP), 3200 (query) |

**Kafka topics:**

| Topic | Notes |
|---|---|
| `order.created` | Published by order-service outbox |
| `payment.settled` | Published by payment-service outbox |
| `payment.failed` | Published by payment-service outbox |
| `audit.dlq` | Dead-letter queue for all services |
| `event.contracts` | Compacted, infinite retention — contract registry backing topic |

All producers use `acks=all`. All consumers use explicit offset commits — never auto-commit.

---

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Go 1.23+
- `make`

### Run the full stack

```bash
git clone https://github.com/yourhandle/gopay.git
cd gopay
cp .env.example .env
make up
```

This boots all three services, three PostgreSQL instances, Redis, Redpanda, Prometheus, Grafana, and Tempo. Migrations run automatically on startup.

### Available make targets

```bash
make up           # build and start full stack
make down         # stop and remove containers
make migrate-all  # run all service migrations
make test         # run all unit and integration tests
make lint         # run golangci-lint across the monorepo
make build        # build all service binaries
```

### Demo flow (end-to-end)

```bash
# 1. Create an order
curl -X POST http://localhost:8081/orders \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"customer_id": "...", "product_id": "...", "amount": "500.00", "currency": "INR"}'

# 2. Watch it flow: order.created → payment.settled → order confirmed
# Open Redpanda console at http://localhost:8080
# Check traces at http://localhost:3000 (Grafana → Tempo)
# Check metrics at http://localhost:9090
```

---

## Security

**JWT authentication** is required on all routes except `/healthz/*` and `/metrics`. Tokens carry `sub` (user UUID), `role`, and `exp` claims.

**RBAC:**
- `customer` — can only create/view orders where `customer_id` matches their JWT `sub`
- `admin` — full access, including `GET /audit/events`

**Rate limiting:** Redis sliding window, 100 requests/minute per IP. Returns `429 Too Many Requests` with a `Retry-After` header. Key: `ratelimit:{ip}:{window_minute}`.

**PII masking:** The `slog` handler automatically redacts any field whose key contains `customer_id`, `card`, `account`, `token`, `pan`, or `aadhaar`. PII never reaches log storage.

**Request-ID:** A UUID is generated at the middleware layer, injected into context, logged on every line, and returned in the `X-Request-ID` response header for full traceability.

---

## Observability

### Prometheus metrics (all services)

| Metric | Labels |
|---|---|
| `http_requests_total` | `method`, `path`, `status` |
| `http_request_duration_seconds` | `method`, `path` |
| `outbox_events_published_total` | — |
| `outbox_relay_lag` | (max unpublished ID − cursor) |
| `kafka_messages_consumed_total` | `topic`, `status` |
| `kafka_dlq_messages_total` | `topic`, `reason` |

### OpenTelemetry Tracing

Trace context is propagated via W3C `TraceContext` headers on HTTP and via Kafka message headers for async flows. Spans are created at controller entry, usecase entry, and repository entry. All spans are exported to **Tempo** and queryable through Grafana.

### Health endpoints

```
GET /healthz/live   → 200 if process is running
GET /healthz/ready  → checks DB ping + Kafka connectivity
```

---

## Project Structure

```
gopay/
├── Makefile
├── docker-compose.yml
├── .env.example
├── proto/                        # Protobuf definitions (future gRPC)
├── pkg/
│   ├── apperrors/                # Sentinel errors and wrapping helpers
│   ├── logger/                   # slog with PII masking handler
│   ├── contracts/                # Event contract registry client and validator
│   ├── outbox/                   # Outbox relay with cursor protocol
│   ├── fingerprint/              # SHA-256 request fingerprinter
│   ├── middleware/               # JWT, RBAC, rate limiter, request-ID
│   ├── pagination/               # Cursor and offset pagination utilities
│   └── crypto/                   # AES-256-GCM and PASETO helpers
├── order-service/
│   ├── cmd/main.go
│   ├── internal/
│   │   ├── config/
│   │   ├── entity/
│   │   ├── dto/
│   │   ├── repository/
│   │   ├── usecase/
│   │   ├── controller/
│   │   ├── provider/
│   │   └── server/
│   ├── migrations/
│   └── Dockerfile
├── payment-service/
│   ├── cmd/main.go
│   ├── internal/
│   │   ├── config/
│   │   ├── entity/
│   │   ├── dto/
│   │   ├── repository/
│   │   ├── usecase/
│   │   ├── controller/
│   │   ├── consumer/
│   │   ├── projection/
│   │   └── provider/
│   ├── migrations/
│   └── Dockerfile
├── audit-service/
│   ├── cmd/main.go
│   ├── internal/
│   │   ├── config/
│   │   ├── entity/
│   │   ├── repository/
│   │   ├── consumer/
│   │   ├── controller/
│   │   └── provider/
│   ├── migrations/
│   └── Dockerfile
└── infra/
    ├── postgres/
    ├── redis/
    ├── redpanda/
    └── observability/
        ├── prometheus.yml
        ├── grafana/
        └── tempo/
```

---

## Error Handling

All sentinel errors are defined in `pkg/apperrors` and wrapped with context at each layer:

```go
return fmt.Errorf("create order usecase: %w", apperrors.ErrInvalidInput)
```

Controllers map sentinel errors to HTTP status codes via `errors.Is`:

| Error | HTTP Status |
|---|---|
| `ErrNotFound` | 404 |
| `ErrConflict` | 409 |
| `ErrInvalidInput` | 400 |
| `ErrUnauthorized` | 401 |
| `ErrForbidden` | 403 |
| default | 500 |

Invalid state machine transitions return `ErrConflict` wrapped with the attempted transition context.

### Dead-Letter Queue

After 3 failed retries (exponential backoff: 1s → 2s → 4s), failed Kafka messages are routed to `audit.dlq`:

```json
{
  "original_topic": "order.created",
  "original_offset": 42,
  "reason": "SCHEMA_VIOLATION | MAX_RETRIES_EXCEEDED",
  "error": "...",
  "retry_count": 3,
  "payload": {},
  "failed_at": "2026-05-14T06:00:00Z"
}
```

The partition is never blocked — offset is committed and processing continues.

---

## API Documentation

OpenAPI specs are available at:

- `order-service`: `http://localhost:8081/swagger/index.html`
- `payment-service`: `http://localhost:8082/swagger/index.html`
- `audit-service`: `http://localhost:8083/swagger/index.html`

---

## License

MIT
