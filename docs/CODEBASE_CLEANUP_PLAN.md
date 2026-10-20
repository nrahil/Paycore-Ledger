# Codebase Cleanup Plan

The refactor is incremental. Preserve API and event contracts, database semantics, and payment/ledger invariants. Avoid creating generic frameworks or extra layers that do not make an existing responsibility easier to understand or test.

## Working rules

1. Review the current diff before each slice; this repository already has a broad uncommitted working tree.
2. Make one cohesive structural change at a time.
3. Keep network calls outside database transactions and keep ledger, payment state, and outbox updates atomic.
4. Compile after each slice; run focused integration tests for behavior changes and the full suite at service milestones.
5. Update this plan and `CODEBASE_MAP.md` as responsibilities move.

## Phases

| Phase | Scope | Status | Completion condition |
|---|---|---|---|
| 0. Baseline and map | Inventory modules, flows, invariants, existing changes, and current responsibility hotspots. | **Complete** | `CODEBASE_MAP.md` reflects the code as it exists; full Gradle verification passes. |
| 1. Design boundaries | Document dependency direction, application/domain/adapter responsibilities, and shared-code constraints; align architecture checks. | **Started** | Boundary rules are explicit and automatically enforced where practical. |
| 2. Payments core | Separate payment authorization, capture/refund operations, recovery, read/write mapping, and payout responsibilities. | **In progress** | Each use case has a clear home; transaction ownership is visible; payment APIs and recovery behavior remain stable. |
| 3. Webhooks | Separate event intake, delivery orchestration, retry claims, persistence, and DLQ replay. | **In progress** | Each delivery/retry state transition has one obvious owner and external HTTP stays outside database transactions. |
| 4. Read model | Separate event validation/buffering, ordered projection, query service, and rebuild orchestration. | **Planned** | Event sequence and projection update rules are isolated and understandable. |
| 5. Reconciliation and gateway | Separate settlement parsing, matching, persistence, and gateway middleware/configuration/routing. | **Planned** | Pure parsing/matching and edge policies are independently navigable. |
| 6. Shared concerns | Reduce duplicated token handling, API errors, configuration defaults, and logging while keeping policies service-specific. | **Planned** | Shared utilities remain small and service authorization/error behavior is consistent. |
| 7. Learning and documentation | Refresh architecture guides and add concise flow/invariant explanations that match implementation. | **Planned** | A new contributor can trace the payment, event, webhook, and settlement flows from the docs. |

## Completed cleanup slices

- Added `CODEBASE_MAP.md` with the current module responsibilities, payment/event flows, invariants, and known decomposition seams.
- Extracted payment row and metadata conversion into `PaymentRowMapper`.
- Extracted two-phase authorization orchestration into `PaymentAuthorizationService`, retaining the `PaymentService` entry point.
- Kept stale PSP operation recovery in `PaymentRecoveryService` and outbound webhook HTTP/signing in `WebhookHttpSender` from the prior cleanup slice.
- Extracted merchant-scoped payment reads and `FOR UPDATE` row locking into `PaymentRepository`; both lifecycle and authorization services use the shared query boundary.
- Made `PaymentRepository` proxyable by Spring after the first full check exposed the CGLIB/final-class issue.
- Verification baseline: `:payments-core:test` passed; full `gradlew check` passed across modules, including payment concurrency/crash tests, event projection/rebuild, webhook retry/DLQ, reconciliation, and Protobuf compatibility. `git diff --check` is clean.

## Next slices

1. Move capture/refund orchestration and operation persistence out of `PaymentService`, preserving its API facade until callers can be migrated safely.
2. Remove construction-time collaborator wiring in recovery by making its state/finalization boundary explicit.
3. Split webhook event intake and retry/DLQ state ownership after reviewing its transaction boundaries.
4. Continue with read-model and reconciliation extraction only after payment and webhook behavior is stable.
