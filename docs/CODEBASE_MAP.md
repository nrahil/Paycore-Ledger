# PayCore Codebase Map

This document describes the current implementation and the main paths through it. It is a navigation aid for development, not a claim that every service is production-ready.

## Repository shape

The root Gradle build contains the JVM applications and shared contracts. The Go gateway is built independently from `gateway/`. Runtime and local demo assets live under `deploy/`, `load/`, and `scripts/`.

| Module | Current responsibility | Main entry points |
|---|---|---|
| `platform-common` | Shared money, IDs, errors, tracing headers, versioned Protobuf event contracts, and constant-time configured-token comparison.  | `Money`, `PaymentEvent` Protobuf |
| `payments-core` | Merchant API, payment lifecycle, idempotency, PSP integration, double-entry ledger, and transactional outbox. | `PaymentController`, `PaymentService`, `LedgerService`, `OutboxPollingRelay` |
| `mock-psp` | Local PSP simulator with operation deduplication and controllable failure modes. | `MockPspController` |
| `read-model-service` | Consumes payment events, buffers gaps, updates payment/merchant projections, serves read APIs, and rebuilds projections. | `ReadModelProjectorService`, `ReadModelQueryService`, `ReadModelController` |
| `webhook-service` | Consumes events, signs and sends merchant webhooks, schedules retries, and manages delivery/DLQ state. | `WebhookDeliveryService`, `WebhookHttpSender`, `WebhookController` |
| `reconciliation` | Accepts settlement CSV input, compares daily PSP movements with ledger movements, and records reports/discrepancies. | `ReconciliationController`, `ReconciliationService` |
| `gateway/` | Go reverse proxy, edge checks, rate limiting, trace propagation, and gateway telemetry. | `main.go` |
| `e2e-tests` | Cross-service scenarios. | `PayCoreEndToEndTest` |

`settings.gradle.kts` defines the included Gradle modules. The root build rejects direct dependencies between deployable service modules; shared JVM contracts belong in `platform-common`.

## Main payment write path

1. The gateway routes merchant requests to `payments-core`. The merchant filter resolves the API key to a merchant identity.
2. `PaymentController` applies request idempotency and delegates lifecycle commands to `PaymentService`. `PaymentRepository` owns merchant-scoped payment lookup and explicit row locking; `PaymentRowMapper` owns persisted-row conversion.
3. Authorization is coordinated by `PaymentAuthorizationService`; capture/refund are coordinated by `PaymentCaptureRefundService`. Both use durable local reservations around PSP calls. Database transactions are released before network calls; finalization records payment state, ledger postings, and outbox events atomically.
4. `LedgerService` validates balanced postings, locks balance rows, writes immutable entries, and updates materialized balances in the caller's transaction.
5. `OutboxPollingRelay` leases eligible rows, publishes Protobuf events to Kafka/Redpanda, and acknowledges successful publication. It preserves per-payment event order.

## Main event and read paths

- `read-model-service` consumes events through `ReadModelKafkaEventListener`, validates them, stores them in its event log, applies contiguous per-payment sequences, and updates query projections. `ReadModelProjectionWriter` owns payment and merchant-summary mutations, rejecting missed payment-row updates and advancing sequence markers for unknown payloads; `ReadModelQueryService` serves paginated payments and merchant summaries; `ReadModelRebuildService` atomically truncates and replays projections from the stored event log. Repeated listener failures are retried and then published to `payments.events.DLT`; a separate listener records DLT entries in PostgreSQL before committing the DLT offset.
- `webhook-service` records event work, maps versioned payment events to merchant JSON through `WebhookPayloadMapper`, performs signed HTTP delivery through `WebhookHttpSender`, and persists the outcome. `WebhookEventWorkRepository` owns incoming-event leases and atomic expired-work batch claims; `WebhookRetryRepository` owns due-retry lease claims and token-guarded completion; `WebhookRetryCoordinator` drives scheduled retries; `WebhookDeliveryRepository` owns delivery history and processed-event markers; `WebhookDlqRepository` owns DLQ persistence and replay-state updates; `WebhookDeliveryOutcomeService` commits multi-row outcomes atomically after HTTP finishes; `WebhookReplayService` coordinates requested replays; `WebhookQueryService` serves delivery, DLQ, and consumer-lag views. Retry and DLQ state is database-backed; redelivery can happen, so consumers should deduplicate by event ID.
- `reconciliation` parses the supplied PSP settlement file and compares each reference/currency/day movement with the corresponding PSP-receivable ledger movement. It records each run and its discrepancies.

## Key invariants to preserve during cleanup

- Never use floating point for money; amounts are integer minor units with explicit currency.
- External PSP and webhook network calls must not hold a database transaction open.
- A payment operation must not post its ledger transaction or event more than once, including after recovery.
- Ledger entries balance per currency; balance changes and related payment/outbox changes commit atomically.
- Outbox publication and downstream event consumption are at least once. Consumers must tolerate duplicates and preserve the event-sequence rules they rely on.
- Merchant and operator identities come from credentials, not caller-supplied merchant IDs.

## Current decomposition status

- Authorization orchestration is in `PaymentAuthorizationService`; capture/refund orchestration and operation persistence are in `PaymentCaptureRefundService`; payout posting and event creation are in `PayoutService`; PSP timeout recovery is in `PaymentRecoveryService`; merchant-scoped payment lookup and row locking are in `PaymentRepository`; payment row and metadata conversion are in `PaymentRowMapper`. `PaymentService` remains the API facade and owns create, void, and read entry points.
- `WebhookPayloadMapper` owns event-to-JSON mapping; `WebhookHttpSender` owns outbound HTTP, signing headers, and delivery-time SSRF checks; `WebhookEventWorkRepository`, `WebhookRetryRepository`, `WebhookDeliveryRepository`, and `WebhookDlqRepository` own durable event-work, retry, delivery-history, and DLQ state respectively. `WebhookDeliveryOutcomeService` owns atomic outcome transactions; `WebhookRetryCoordinator` owns scheduled retry orchestration; `WebhookReplayService` owns requested replay orchestration; `WebhookQueryService` owns read use cases; `WebhookDeliveryService` handles intake and individual delivery decisions.
- `ReadModelProjectorService` owns event intake and sequence draining; `ReadModelProjectionWriter` owns projection mutations; `ReadModelQueryService` owns payment pagination and merchant summaries; `ReadModelRebuildService` owns the truncate-and-replay transaction. Unknown payloads advance the aggregate sequence without modifying recognized payment fields, and failed guarded updates roll back the event transaction.
- `SettlementCsvParser` validates the settlement file; `ReconciliationMatcher` applies status/currency/amount and missing-row policy; `ReconciliationRepository` owns settlement ledger reads and report persistence; `ReconciliationService` owns the transaction and use-case orchestration.
- The Go gateway keeps process configuration/startup and request middleware in `main.go`; `route_policy.go` owns upstream and edge path policy; `rate_limiter.go` owns per-key token buckets and synchronized eviction. Tracing setup remains in `main.go`.

Each service keeps route authorization and HTTP response policy local, while `ServiceTokenValidator` centralizes the shared constant-time token comparison primitive. Idempotency claim failures remove their request-local future before propagating. Refactors preserve the invariants above and keep public APIs and stored event formats stable unless a separate change is intended.

## Read-model failure behavior

Malformed Protobuf payloads and conflicting events that claim an occupied `(payment_id, event_seq)` are stored in `read_model_poison_events` and the listener returns normally. A structurally valid event that cannot be projected rolls back the database transaction and propagates through the Kafka adapter; after bounded retries it is published to `payments.events.DLT`. The DLT registry consumer stores the original topic, partition, offset, key, payload, and failure details in `read_model_dead_letters` before acknowledging the DLT record. Operators can inspect records with `GET /v1/admin/read-model/dead-letters` and replay one with `POST /v1/admin/read-model/dead-letters/{id}/replay`. Replay republishes the unchanged payload to `payments.events` with its original message key; the projector's durable `event_seq` gap buffer places it logically in sequence and drains later buffered events. Fix the cause before replaying; a still-invalid event will fail and return to the DLT. DLT registry persistence retries indefinitely on database errors so the broker record is not acknowledged before it is stored.
