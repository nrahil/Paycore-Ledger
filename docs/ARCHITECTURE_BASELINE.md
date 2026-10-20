# PayCore Architecture Baseline

This document is the working target for hardening PayCore. It describes intended behavior and acceptance criteria; it does not claim that the current implementation already meets them. Update it when a design decision changes, then keep the API, code, migrations, tests, and demo aligned with it.

## Project scope

PayCore is a local payment orchestration and ledger system. It simulates a payment provider and bank settlement; it does not process real cards, connect to a real bank, or claim PCI compliance or cloud-scale availability.

The money path remains in one transactional service and one PostgreSQL consistency boundary: merchant/payment state, idempotency records, ledger postings, and outbox events. The gateway is an edge adapter. The PSP is a fault-injection simulator. Webhook delivery, read projections, and reconciliation are downstream services with explicit input contracts and independent recovery behavior.

## Required behavior and invariants

### Payment operations

- State transitions are explicit and enforced in application code and database constraints.
- Every mutating client request has a merchant-scoped idempotency key. Same key and same canonical request returns the same operation/result; same key with a different request is rejected.
- Every external capture/refund operation has a durable operation ID that is stable across retries and is sent to the PSP as its idempotency identity.
- A timeout is an unknown outcome, not proof of failure. A recovery worker replays stale capture/refund operations with their original PSP idempotency identity and converges local state without duplicating the side effect.
- External PSP calls run outside database transactions. Local reservations and finalization each use short transactions.

### Ledger and balances

- Money is represented as integer minor units and postings never mix currencies.
- Each ledger transaction balances debits and credits in its currency, has at least two entries, and is append-only. Corrections use reversing postings.
- Ledger entries, account balance changes, payment state changes, and the corresponding outbox event commit atomically when they describe the same business operation.
- Account balances are projections of ledger entries and are checked against them.
- Refund posting and fee treatment must be consistent. **Decision:** return the platform fee proportionally with refunds, so a full refund reverses the full original capture and does not require the merchant to have unrelated funds available. Partial refunds reverse the matching proportion of the original fee, with rounding handled deterministically and the final refund settling any remainder.

### Events and consumers

- The outbox is written in the same database transaction as its business state change. Delivery is at least once; consumers deduplicate by event ID.
- Per-payment sequence is authoritative. Relay workers preserve that sequence for each payment, while consumers tolerate duplicates and out-of-order arrivals.
- Required lifecycle changes, including payment creation and voiding, have defined event contracts.
- Each consumer commits event deduplication and its projection/delivery bookkeeping atomically. External HTTP delivery is not performed while holding a long database transaction.
- Webhook retries have an active, durable scheduler or retry-topic consumer, bounded attempts, observable status, and an explicit replay path.

### Identity, authorization, and service boundaries

- Merchant identity comes from a validated API key or a trusted service identity, never from an untrusted merchant ID header/query parameter. Read-model and webhook APIs derive the merchant ID from the validated key.
- Every service enforces authorization for its own endpoints. `/v1/admin/**` requires a separately configured `PAYCORE_ADMIN_TOKEN`; missing configuration disables admin access. Direct service access does not bypass these checks.
- Reads and writes are scoped to the authenticated merchant. Internal ingestion and rebuild endpoints are private to trusted callers.
- Webhook endpoint configuration has one authoritative owner and is available to the dispatcher through an explicit, tested integration.
- Outbound webhook requests block private, loopback, link-local, metadata, and other prohibited destinations at connection time, including redirect targets.

### Read model, reconciliation, and operations

- The read model is explicitly eventually consistent, tenant-scoped, replayable, and rebuildable from a durable event source. Summaries describe exactly what they include, including payout effects if they claim to represent available balance.
- Reconciliation matches daily signed PSP movements by reference and currency against PSP-receivable ledger postings created during the requested UTC date. Captures are positive movements and later refunds are negative movements on their own posting dates. Duplicate or malformed settlement rows are rejected, not silently discarded.
- Local deployment starts the services and infrastructure with consistent ports, service discovery, credentials, event listeners, and outbox publishing enabled where required.
- Health, readiness, metrics, backups, restores, and failure recovery are verified with repeatable commands.

## Implementation sequence

1. Preserve the current worktree and establish a reproducible build/test baseline.
2. Close authentication, authorization, and tenant-isolation gaps.
3. Make capture/refund operations durable, PSP-idempotent, and recoverable under the approved proportional fee-reversal policy.
4. Make outbox, consumers, webhook registration, and retries reliable end to end.
5. Correct read-model semantics and ledger-based settlement reconciliation.
6. Make local and Kubernetes deployment, observability, backup, and restore match the supported scope.
7. Run the acceptance suite from a clean environment and regenerate the verification evidence.

## Baseline record

- Working tree already contains uncommitted payment-flow changes and a new migration/test. Preserve and review these; do not reset or overwrite them during hardening.
- Java 21, Gradle 8.10.2, Go, and Docker are available in the current environment.
- `go test -race ./...` passed for the gateway.
- Fresh full Gradle `check` initially found that distinct-key concurrent capture requests were all reported successful while one capture operation should win. Step 3 corrected this behavior and added capture/refund operation invariants.
- The other reported Gradle tests passed, including crash consistency, parallel idempotency, live Redis/Redpanda integration, outbox relay, core integration, architecture boundaries, PSP operation failure handling, and ledger properties.

## Security hardening record

- Merchant ledger reads now include the authenticated merchant ID in the database query, so a valid key cannot read another merchant's ledger entries.
- Read-model and webhook APIs validate the merchant API key and derive tenant identity from it; caller-supplied merchant IDs are no longer authoritative. Merchant key verification reads the core `merchants` table in the project's shared PostgreSQL database, matching the current deployment topology.
- `/v1/admin/**` uses the separate `X-PayCore-Admin-Token` credential and fails closed when `PAYCORE_ADMIN_TOKEN` is unset. The mock PSP also requires `X-PayCore-Internal-Token` for PSP operations, supplied by payments-core from `PAYCORE_INTERNAL_TOKEN`; fault configuration uses the admin token. Read-model ingestion uses the internal token as well.
- OpenAPI and README security descriptions were updated to distinguish merchant, operator, and service credentials.
- Verification after these changes: payments-core integration and PSP failure tests, read-model integration tests, webhook integration tests, reconciliation integration tests, and mock-PSP integration tests passed. `git diff --check` reported no whitespace errors; Git emitted only line-ending normalization warnings for existing Windows working-tree files.

## Capture/refund hardening record

- Capture claims now transition only from `AUTHORIZED`; concurrent or subsequent requests with distinct idempotency keys conflict instead of reusing a pending capture or reporting a duplicate success.
- Capture and refund PSP calls carry the UUID of their durable `payment_operations` row. The mock PSP deduplicates by that operation identity, and the embedded PSP client retains operation results for safe in-process retries.
- A scheduled operation resolver replays stale `IN_PROGRESS` capture/refund calls with the same operation ID, then runs normal atomic ledger/state/outbox finalization. Finalizers recheck operation state under the payment lock so two resolver instances cannot post the same operation twice.
- Capture fee is stored on its operation row. Refund fee reversals are allocated from cumulative refunded gross using integer arithmetic, with the final refund taking any rounding remainder. Ledger refund postings debit merchant payable for net refund, debit platform fee revenue for the reversed fee, and credit PSP receivable for gross refund.
- `refund.processed` now includes `fee_reversed_amount`; read-model fee totals and the refund API response reflect the reversed fee. OpenAPI and Protobuf compatibility checks include the added field.
- Verification: Java compilation; mock-PSP retry deduplication; payments-core integration, PSP failure, capture/refund crash-recovery, and concurrency tests; read-model integration/rebuild; and Protobuf compatibility checks passed. The concurrency test recognizes both `422` over-refund and `409` terminal-state conflicts as valid rejections. The test-only unsafe refund mode was removed.

## Event delivery hardening record

- The polling outbox now claims rows with a short leased database claim, publishes outside the claim transaction, and acknowledges only while the claim token still matches. Expired leases recover process crashes; publish errors clear the lease, record an error, and apply capped exponential backoff. The claim query blocks a later event while any earlier unpublished sequence exists for that aggregate.
- The webhook dispatcher now reads endpoint URL/secret from the canonical `merchants` record shared with payments-core. The authenticated endpoint registration API updates that record; missing endpoints create a visible DLQ record rather than being reported as delivered. Retry polling is enabled on a schedule and due work is claimed with leases before HTTP calls. Retry state is database-authoritative; optional Kafka retry-topic sends are not required for progress.
- Initial webhook event work is now persisted and leased before the outbound request. Expired work is recovered by a scheduled worker, so a process crash does not strand an event. Delivery remains at least once across a crash after the merchant accepts the request and before local acknowledgement; merchants should deduplicate using the stable event ID header.
- The read-model consumer serializes each payment's projection with a transaction-scoped advisory lock, including the first event before a `payment_view` row exists. Malformed protobuf/metadata and conflicting event IDs for an occupied payment sequence are durably quarantined and deduplicated by payload hash.
- The Kubernetes workload example enables the payments-core outbox relay, points read-model and webhook listeners at Redpanda, uses the shared writable PostgreSQL endpoint required by their migrations/projections, and uses the actual configured read-model listener property.
- Verification completed: outbox relay crash/ordering and broker-error/backoff integration tests passed; webhook delivery, HMAC, retry tiers, DLQ replay, and SSRF integration tests passed; read-model ordering, rebuild, authorization, and malformed-event quarantine tests passed. A failed first read-model run exposed a JDBC mapping error on PostgreSQL's void-returning advisory-lock function; the corrected full focused rerun passed. The webhook service emitted a shutdown-time scheduled-worker connection warning as its test container was stopping; the test task completed successfully.

## Read-model and reconciliation hardening record

- Merchant summaries now group amounts by currency, include completed payouts in daily and total figures, and calculate remaining payable per currency after fees and refunds. Payout events are projected idempotently and included in a rebuild from the durable event log.
- Settlement matching now derives the expected net from ledger postings to PSP-receivable accounts, scopes missing-file checks to capture ledger transactions on the requested UTC settlement date, compares currency, rejects duplicate/malformed/out-of-date rows, and surfaces invalid feeds as HTTP 400. The mock PSP export now filters by settlement date, excludes authorizations, and emits currency.
- Verification: read-model integration/rebuild tests passed including payout projection, replay, and conflicting-sequence assertions. Reconciliation Testcontainers integration tests passed, covering all discrepancy types, ledger-derived expected amounts, currency mismatch, duplicate references, and settlement-date validation. Mock PSP settlement export tests passed for the date-filtered currency-bearing feed.

## Deployment and operations hardening record

- The local Compose stack now persists PostgreSQL, Redis, Prometheus, and Grafana state; Grafana provisions its Prometheus datasource and dashboard. The Prometheus configuration targets the mock PSP in addition to application services.
- Kubernetes app manifests now include internal Services for every application, the gateway NodePort, config-driven service/broker URLs, CloudNativePG-generated database credentials, and required runtime tokens from a Kubernetes Secret. Mock PSP and reconciliation deployments are included. Database-backed service readiness checks query PostgreSQL. CNPG operator and a reachable Redpanda-compatible broker remain cluster prerequisites.
- The backup/restore verification script creates a retained SQL dump, refuses to overwrite a backup or existing scratch database, restores into an isolated scratch database, compares ledger transaction counts, and checks per-transaction/per-currency balance.
- Webhook DLQ depth is now exported from durable `dlq_events` state for the existing Prometheus alert.
- Flyway histories are separate per service while the current topology shares one database/schema. Each service now baselines at version 0 so its V1 migration runs instead of being incorrectly marked as already applied. A fresh local database boot confirmed all service migrations apply in sequence.
- Webhook, read-model, and reconciliation now expose Prometheus and health-probe actuator endpoints, matching the existing scrape configuration.
- The live Redis/Kafka integration test now drains successive outbox batches. The relay intentionally only claims the earliest unpublished event per payment in each batch to preserve ordering; assuming all three payment lifecycle events are claimable at once made the test fail even though relay behavior was correct.
- Verification: full Gradle `check` and `bootJar` passed with Java 21 and Docker Desktop's active `desktop-linux` pipe. This includes the Testcontainers integration suites, Redis outage fallback, Kafka Protobuf delivery, crash/concurrency torture tests, reconciliation, webhook, read-model rebuild, PSP failure/recovery, ledger properties, architecture boundary checks, and Proto compatibility. `go test -race ./...` passed. Docker Compose config, Kubernetes/OpenAPI/Prometheus YAML, and the backup PowerShell script parsed; `git diff --check` found no whitespace errors (Git only warned about Windows line-ending normalization).
- Local runtime verification: the services and infrastructure were started locally, readiness returned HTTP 200 for the JVM services, and all six Prometheus targets (gateway, payments-core, webhook, read-model, reconciliation, mock PSP) reported `up`. The live demo flow and backup/restore flow had passed earlier in this work session against local disposable/demo data; cluster-side Kubernetes validation was not run, so the manifest remains an example requiring CNPG and a reachable Redpanda-compatible broker.

## Observability and local load-demo record

- Spring Boot services export Micrometer traces over OTLP/HTTP; the Go edge gateway now creates an OpenTelemetry HTTP span and exports OTLP/gRPC to Jaeger. W3C trace context is extracted from incoming requests and injected on the gateway-to-core call, linking the gateway, payment-core, and mock-PSP spans.
- The Grafana dashboard now uses live HTTP request counters and histogram buckets for request rate and p50/p95/p99 latency. Its payment request and gateway panels use metrics that are emitted by the running services. The local dashboard provisioning was restarted and checked against live Prometheus data.
- The local k6 scenario exercised payment creation, authorization, and capture through the gateway for 30 seconds with 2 virtual users. It completed 180 flows / 540 HTTP requests, passed all 540 checks, recorded 0 failed HTTP requests, and measured 274.56 ms p95 capture latency. All six application scrape targets were `up`. This is a local smoke/load demo, not a production benchmark.
- Jaeger verified a captured trace with four spans across three services (gateway, payments-core, mock PSP). The screenshots and summarized metrics are embedded in the README and stored under `docs/observability-assets/`.
- Verification: `go test -race ./...` and `go vet ./...` passed after gateway tracing changes. The Gradle `check bootJar` build passed; the test reports contained 41 tests with zero failures, and all service boot JARs were rebuilt. `git diff --check` reported no whitespace errors (only Windows line-ending normalization warnings). Kubernetes cluster validation was not part of this local observability check.
