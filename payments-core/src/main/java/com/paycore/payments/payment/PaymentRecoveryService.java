package com.paycore.payments.payment;

import com.paycore.common.clock.ClockProvider;
import com.paycore.payments.outbox.OutboxWriter;
import com.paycore.payments.psp.PspClient;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** Resolves PSP operations whose outcome was unknown when the original request ended. */
final class PaymentRecoveryService {

    private static final Logger log = LoggerFactory.getLogger(PaymentRecoveryService.class);

    private final PaymentService paymentService;
    private final JdbcClient jdbcClient;
    private final PspClient pspClient;
    private final OutboxWriter outboxWriter;
    private final ClockProvider clockProvider;
    private final TransactionTemplate requiresNewTx;

    PaymentRecoveryService(
            PaymentService paymentService,
            JdbcClient jdbcClient,
            PspClient pspClient,
            OutboxWriter outboxWriter,
            ClockProvider clockProvider,
            PlatformTransactionManager txManager) {
        this.paymentService = paymentService;
        this.jdbcClient = jdbcClient;
        this.pspClient = pspClient;
        this.outboxWriter = outboxWriter;
        this.clockProvider = clockProvider;
        this.requiresNewTx = new TransactionTemplate(txManager);
        this.requiresNewTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    int resolveStuckAuthorizingPayments(Duration threshold) {
        Instant cutoff = clockProvider.now().minus(threshold);
        List<StuckAuthorization> stuckPayments = jdbcClient.sql("""
                SELECT id, merchant_id, psp_ref
                FROM payments
                WHERE status = 'AUTHORIZING'
                  AND updated_at <= :cutoff
                ORDER BY updated_at ASC
                LIMIT 100
                """)
                .param("cutoff", Timestamp.from(cutoff))
                .query((rs, rowNum) -> new StuckAuthorization(
                        rs.getObject("id", UUID.class),
                        rs.getObject("merchant_id", UUID.class),
                        rs.getString("psp_ref")))
                .list();

        int resolvedCount = 0;
        for (StuckAuthorization stuck : stuckPayments) {
            if (stuck.pspRef() == null || stuck.pspRef().isBlank()) continue;
            PspClient.PspStatusLookupResult lookup = pspClient.lookupByPspRef(stuck.pspRef());
            if (lookup.state() == PspClient.PspLookupState.LOOKUP_ERROR) continue;

            Boolean converged = requiresNewTx.execute(status -> {
                Instant now = clockProvider.now();
                Timestamp nowTs = Timestamp.from(now);
                if (lookup.state() == PspClient.PspLookupState.AUTHORIZED) {
                    int updated = jdbcClient.sql("""
                            UPDATE payments
                            SET status = 'AUTHORIZED', version = version + 1,
                                event_seq = event_seq + 1, updated_at = :updatedAt
                            WHERE id = :id AND status = 'AUTHORIZING'
                            """)
                            .param("updatedAt", nowTs).param("id", stuck.id()).update();
                    if (updated == 1) {
                        Payment after = paymentService.getPayment(stuck.merchantId(), stuck.id());
                        outboxWriter.writePaymentAuthorized(after.id(), after.merchantId(), after.eventSeq(),
                                after.amount(), after.currency(), after.pspRef());
                        return true;
                    }
                } else {
                    int updated = jdbcClient.sql("""
                            UPDATE payments
                            SET status = 'FAILED', version = version + 1,
                                event_seq = event_seq + 1, updated_at = :updatedAt
                            WHERE id = :id AND status = 'AUTHORIZING'
                            """)
                            .param("updatedAt", nowTs).param("id", stuck.id()).update();
                    if (updated == 1) {
                        Payment after = paymentService.getPayment(stuck.merchantId(), stuck.id());
                        outboxWriter.writePaymentFailed(after.id(), after.merchantId(), after.eventSeq(),
                                after.amount(), after.currency(), after.pspRef(), "RESOLVED_" + lookup.state().name());
                        return true;
                    }
                }
                return false;
            });

            if (Boolean.TRUE.equals(converged)) {
                resolvedCount++;
                log.info("Resolved stuck AUTHORIZING payment {} via PSP lookup ({})", stuck.id(), lookup.state());
            }
        }
        return resolvedCount;
    }

    int resolveStuckCaptureRefundOperations(Duration threshold) {
        Instant cutoff = clockProvider.now().minus(threshold);
        List<PendingOperation> pending = jdbcClient.sql("""
                SELECT id, merchant_id, payment_id, operation_type, amount
                FROM payment_operations
                WHERE status = 'IN_PROGRESS' AND created_at <= :cutoff
                ORDER BY created_at ASC LIMIT 100
                """)
                .param("cutoff", Timestamp.from(cutoff))
                .query((rs, rowNum) -> new PendingOperation(
                        rs.getObject("id", UUID.class), rs.getObject("merchant_id", UUID.class),
                        rs.getObject("payment_id", UUID.class), rs.getString("operation_type"),
                        rs.getLong("amount")))
                .list();

        int resolved = 0;
        for (PendingOperation operation : pending) {
            try {
                Payment payment = paymentService.getPayment(operation.merchantId(), operation.paymentId());
                if ("CAPTURE".equals(operation.type())) {
                    PspClient.PspCaptureResult result = pspClient.capture(new PspClient.PspCaptureRequest(
                            operation.paymentId(), operation.merchantId(), operation.id(), payment.pspRef(),
                            operation.amount(), payment.currency()));
                    if (result.outcome() == PspClient.PspOutcome.AUTHORIZED) {
                        requiresNewTx.execute(status -> paymentService.finalizeCapture(
                                operation.merchantId(), operation.paymentId(), operation.id(), result));
                        resolved++;
                    }
                } else if ("REFUND".equals(operation.type())) {
                    PspClient.PspRefundResult result = pspClient.refund(new PspClient.PspRefundRequest(
                            operation.paymentId(), operation.merchantId(), operation.id(), payment.pspRef(),
                            operation.amount(), payment.currency()));
                    if (result.outcome() == PspClient.PspOutcome.AUTHORIZED) {
                        requiresNewTx.execute(status -> paymentService.finalizeRefund(
                                operation.merchantId(), operation.paymentId(), operation.id(),
                                new PaymentService.RefundRequest(operation.amount(), null), result));
                        resolved++;
                    }
                }
            } catch (RuntimeException ex) {
                log.warn("Could not resolve PSP operation {}: {}", operation.id(), ex.getMessage());
            }
        }
        return resolved;
    }

    private record StuckAuthorization(UUID id, UUID merchantId, String pspRef) {}

    private record PendingOperation(UUID id, UUID merchantId, UUID paymentId, String type, long amount) {}
}
