# Gopay

Backend-only fintech payment simulation built in Go.

## Services

- order-service
- payment-service
- audit-service

## Core Patterns

- Transactional outbox with Redis cursor
- Implicit idempotency using request fingerprinting
- Event contract registry
- Payment capability projection

## Local Development

```bash
make up
make test
make down
```