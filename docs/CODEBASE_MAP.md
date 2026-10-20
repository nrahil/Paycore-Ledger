# PayCore Codebase Map

This document describes the current implementation and the main paths through it. It is a navigation aid for development, not a claim that every service is production-ready.

## Repository shape

The root Gradle build contains the JVM applications and shared contracts. The Go gateway is built independently from `gateway/`. Runtime and local demo assets live under `deploy/`, `load/`, and `scripts/`.

| Module | Current responsibility | Main entry points |
|---|---|---|
| `platform-common` | Shared money, IDs, errors, tracing headers, and versioned Protobuf event contracts. | `Money`, `PaymentEvent` Protobuf |
| `payments-core` | Merchant API, payment lifecycle, idempotency, PSP integration, double-entry ledger, and transactional outbox. | `PaymentController`, `PaymentService`, `LedgerService`, `OutboxPollingRelay` |
| `mock-psp` | Local PSP simulator with operation deduplication and controllable failure modes. | `MockPspController` |
| `read-model-service` | Consumes payment events, buffers gaps, updates payment/merchant projections, serves read APIs, and rebuilds projections. | `ReadModelProjectorService`, `ReadModelController` |
| `webhook-service` | Consumes events, signs and sends merchant webhooks, schedules retries, and manages delivery/DLQ state. | `WebhookDeliveryService`, `WebhookHttpSender`, `WebhookController` |
| `reconciliation` | Accepts settlement CSV input, compares daily PSP movements with ledger movements, and records reports/discrepancies. | `ReconciliationController`, `ReconciliationService` |
| `gateway/` | Go reverse proxy, edge checks, rate limiting, trace propagation, and gateway telemetry. | `main.go` |
| `e2e-tests` | Cross-service scenarios. | `PayCoreEndToEndTest` |

`settings.gradle.kts` defines the included Gradle modules. The root build rejects direct dependencies between deployable service modules; shared JVM contracts belong in `platform-common`.

## Main payment write path

1. The gateway routes merchant requests to `payments-core`. The merchant filter resolves the API key to a merchant identity.
2. `PaymentController` applies request idempotency and delegates lifecycle commands to `PaymentService`. `PaymentRepository` owns merchant-scoped payment lookup and explicit row locking; `PaymentRowMapper` owns persisted-row conversion.
3. Authorization, capture, and refund use durable local reservations around PSP calls. Database transactions are released before network calls; finalization records payment state, ledger postings, and outbox events atomically.
4. `LedgerService` validates balanced postings, locks balance rows, writes immutable entries, and updates materialized balances in the caller's transaction.
5. `OutboxPollingRelay` leases eligible rows, publishes Protobuf events to Kafka/Redpanda, and acknowledges successful publication. It preserves per-payment event order.

## Main event and read paths

- `read-model-service` validates incoming events, stores them in its event log, applies contiguous per-payment sequences, and updates query projections. Its API serves merchant payment lists and summaries; rebuild replays the stored events.
- `webhook-service` records event work, looks up the merchant endpoint, performs signed HTTP delivery, and persists the outcome. Retry and DLQ state is database-backed; redelivery can happen, so consumers should deduplicate by event ID.
- `reconciliation` parses the supplied PSP settlement file and compares each reference/currency/day movement with the corresponding PSP-receivable ledger movement. It records each run and its discrepancies.

## Key invariants to preserve during cleanup

- Never use floating point for money; amounts are integer minor units with explicit currency.
- External PSP and webhook network calls must not hold a database transaction open.
- A payment operation must not post its ledger transaction or event more than once, including after recovery.
- Ledger entries balance per currency; balance changes and related payment/outbox changes commit atomically.
- Outbox publication and downstream event consumption are at least once. Consumers must tolerate duplicates and preserve the event-sequence rules they rely on.
- Merchant and operator identities come from credentials, not caller-supplied merchant IDs.

## Current decomposition status

- Authorization orchestration is in `PaymentAuthorizationService`; PSP timeout recovery is in `PaymentRecoveryService`; merchant-scoped payment lookup and row locking are in `PaymentRepository`; payment row and metadata conversion is in `PaymentRowMapper`. `PaymentService` still coordinates capture/refund/payout and owns much of their SQL.
- `WebhookHttpSender` owns outbound HTTP, signing headers, and delivery-time SSRF checks. `WebhookDeliveryService` still coordinates consumption, persistence, retries, replay, and query operations.
- `ReadModelProjectorService` currently owns ingestion, ordered projection, rebuilding, and read queries.
- `ReconciliationService` currently owns file parsing, comparison logic, and report persistence.
- The Go gateway currently combines configuration, routing, middleware, rate limiting, telemetry setup, and server startup in `main.go`.

These are the planned cleanup seams. Refactors should preserve the invariants above and keep public APIs and stored event formats stable unless a separate change is intended.
