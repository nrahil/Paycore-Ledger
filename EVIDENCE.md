# PayCore Verification Evidence

This file summarizes what the current source implements and what the repository records as verified. Test coverage, a recorded passing run, and a performance measurement are different kinds of evidence; they are labeled separately below. This is a local portfolio project, not a production certification.

## 1. Financial integrity controls

The payments-core PostgreSQL migration and `LedgerService` implement these controls:

- `ledger_entries` is range-partitioned by `created_at`; the initial schema creates monthly partitions for September through December 2026 plus a default partition. Future monthly partitions are not automatically provisioned by that migration.
- A database trigger rejects updates and deletes to ledger entries. Corrections use compensating postings.
- Deferred constraint trigger `trg_ledger_txn_balanced` checks at commit that every ledger transaction has at least two entries and that debit and credit totals balance.
- `LedgerService.post` checks the totals before insertion, locks `account_balances` rows in ascending account-ID order, and prevents a merchant payable balance from going negative.
- The invariant endpoint checks global debit/credit balance, account-balance drift, and related payment/account invariants.

The optional 16-way mode shards both PSP-receivable and platform-fee system accounts using `Math.floorMod(paymentId.hashCode(), 16)`. It is disabled by default. This is an implementation option and a benchmark scenario, not a claim that production-scale contention has been eliminated.

## 2. Test coverage in the current source

The following tests exist in the repository. The listed counts are test inputs and assertions, not independent benchmark results.

| Scenario | What the test asserts | Test location / scope |
| :--- | :--- | :--- |
| Same-key idempotency concurrency | 500 parallel capture requests with one key return the same payment result and create one ledger transaction; 500 same-key requests with different payloads are rejected. | `ConcurrencyAndCrashTortureTest`, tagged `stretch` |
| Concurrent capture | Of 200 requests using distinct idempotency keys for one payment, exactly one succeeds and 199 conflict. | `ConcurrencyAndCrashTortureTest`, tagged `stretch` |
| Concurrent refunds | 100 concurrent requests each refunding 200 paise against a 1,000-paise capture produce five successful refunds; the other 95 are counted as validation rejections or state conflicts. The test has no unlocked-baseline comparison. | `ConcurrencyAndCrashTortureTest`, tagged `stretch` |
| Crash/fault recovery | 1,000 randomized iterations inject PSP faults or simulated process-crash points, then run the recovery resolver and assert no stuck authorizations, missing outbox sequence entries, or financial invariant violations. | `ConcurrencyAndCrashTortureTest`, tagged `stretch` |
| Ledger properties | 25 seeded property trials generate 15–35 operations across three merchants and assert ledger/account/payment invariants. | `LedgerPropertyTest` |
| Read-model Kafka DLT and replay | A broker-backed integration test drives a projection failure through retries to the DLT, verifies durable registration, repairs the projection condition, replays the original keyed event, and verifies that the sequence gap drains. | `ReadModelDeadLetterKafkaIntegrationTest` with Embedded Kafka and PostgreSQL Testcontainers |
| Projection ordering/rebuild | Tests cover event-sequence buffering, duplicate handling, list/summary queries, and rebuild behavior. | `ReadModelIntegrationTest` |
| Webhook delivery | Tests cover signatures, target-address validation, retry scheduling, dead-lettering, and replay. Webhook retry/DLQ records are PostgreSQL-backed, separate from the read-model Kafka DLT. | `WebhookServiceIntegrationTest` |
| Settlement matching | Tests cover date-scoped capture/refund movements, discrepancy classification, CSV parsing, currency, duplicate references, and invalid dates. | `ReconciliationIntegrationTest` |
| Gateway policy and concurrency | Tests cover API-key presence checks, idempotency-header enforcement, route selection, trace propagation, rate limiting, and concurrent limiter behavior. The gateway itself does not validate API-key hashes; the receiving service does. | `gateway/gateway_test.go`, `gateway/rate_limiter_test.go` |
| Toxiproxy transaction interruption | The test injects a network cut during a database transaction and asserts that the uncommitted partial ledger posting is rolled back. | `PayCoreEndToEndTest` in `e2e-tests` |

The concurrency torture and A/B benchmark classes are tagged `stretch` and excluded from the default test gate. Run them explicitly with `./gradlew test -PincludeStretchBenchmarks=true`; they are machine-sensitive. The recorded latest stretch attempt had two timeouts and a prior full check showed database-pool saturation during a capture burst. Do not treat the stretch suite as a clean performance result.

## 3. Previously recorded focused verification

`docs/CODEBASE_CLEANUP_PLAN.md` records these focused checks as passing during the cleanup work:

- Payments-core lifecycle, PSP failure, outbox relay, property, and architecture-boundary tests.
- Platform-common, webhook, reconciliation, and mock-PSP module tests.
- Read-model ordering/rebuild tests and the broker-backed DLT/replay integration test after that feature was added.
- Go gateway race tests and `go vet`.

Those are historical verification notes, not a claim that every test was rerun while this file was corrected. A complete clean run of the entire Gradle suite and all stretch tests is not claimed here.

## 4. Recorded local k6 write-path run

Machine-readable result: [`docs/observability-assets/k6-10vu-2m-results.json`](docs/observability-assets/k6-10vu-2m-results.json).

- **Workload:** 10 constant virtual users for 120 seconds, each creating, authorizing, and capturing one INR payment through the Go gateway and local mock PSP.
- **Recorded result:** 4,241 completed flows (35.28 flows/s), 12,723 HTTP requests (105.84 requests/s), zero failed HTTP requests, and 12,723 checks passed.
- **Capture request latency:** p50 140.51 ms; p95 330.81 ms; maximum 2,724.94 ms. The configured p95 threshold was 2,000 ms and passed. A p95 pass does not imply every request completed under that threshold.
- **Financial check:** the post-run invariant report recorded zero imbalance, unbalanced transactions, negative merchant accounts, mismatched balance accounts, and ledger drift.
- **Prometheus cross-check:** recorded 4,241 successful create, authorize, and capture requests in payments-core, 4,241 mock-PSP authorization/capture calls, and a zero drift gauge.
- **Scope:** one Windows laptop (12th Gen Intel Core i5-12500H, 15.7 GB RAM), local Docker Compose infrastructure. Gateway, payments-core, and mock PSP application services were active; webhook, read-model, and reconciliation targets were down. Refunds, webhook delivery, and consumer lag were not measured. This is a local write-path load result, not a production capacity claim.

The separate earlier two-VU/30-second smoke-test screenshots are not evidence for the 10-VU result. See the README for the screenshots and reproduction command.

## 5. Claims intentionally not made

- No naive unlocked-refund comparison is present in the current test source.
- The Go gateway checks API-key presence and applies edge policies; merchant API-key validation occurs in the receiving JVM service.
- The read-model Kafka DLT and webhook PostgreSQL DLQ are different mechanisms.
- Settlement reconciliation is invoked by uploading settlement data; this project does not automatically fetch PSP files or schedule a T+1 run.
- The k6 run did not test the full asynchronous consumer path or production-scale capacity.
- The historical A/B timing figures from older versions of this report are omitted because the current recorded stretch run was not clean enough to support those claims.
