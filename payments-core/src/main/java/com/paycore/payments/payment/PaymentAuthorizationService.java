package com.paycore.payments.payment;

import com.paycore.common.clock.ClockProvider;
import com.paycore.common.errors.PayCoreException;
import com.paycore.common.ids.UuidV7;
import com.paycore.payments.outbox.OutboxWriter;
import com.paycore.payments.psp.PspClient;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Consumer;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/** Coordinates the two local transactions and external PSP call for authorization. */
@Service
public class PaymentAuthorizationService {

    private final JdbcClient jdbcClient;
    private final PaymentRepository paymentRepository;
    private final OutboxWriter outboxWriter;
    private final PspClient pspClient;
    private final ClockProvider clockProvider;
    private final TransactionTemplate requiresNewTx;
    private volatile Consumer<String> crashHook = stage -> {};

    public PaymentAuthorizationService(
            JdbcClient jdbcClient,
            PaymentRepository paymentRepository,
            OutboxWriter outboxWriter,
            PspClient pspClient,
            ClockProvider clockProvider,
            PlatformTransactionManager transactionManager) {
        this.jdbcClient = jdbcClient;
        this.paymentRepository = paymentRepository;
        this.outboxWriter = outboxWriter;
        this.pspClient = pspClient;
        this.clockProvider = clockProvider;
        this.requiresNewTx = new TransactionTemplate(transactionManager);
        this.requiresNewTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public Payment authorizePayment(UUID merchantId, UUID paymentId) {
        AuthClaim claim = requiresNewTx.execute(status -> claimAuthorization(merchantId, paymentId));
        if (claim.alreadyAuthorized()) return claim.payment();

        crashHook.accept("AFTER_TX1_BEFORE_PSP");
        PspClient.PspAuthResult result = pspClient.authorize(new PspClient.PspAuthRequest(
                paymentId, merchantId, claim.attemptNo(), claim.pspRef(),
                claim.payment().amount(), claim.payment().currency()));
        crashHook.accept("AFTER_PSP_BEFORE_TX2");

        return requiresNewTx.execute(status -> finalizeAuthorization(merchantId, paymentId, claim, result));
    }

    private AuthClaim claimAuthorization(UUID merchantId, UUID paymentId) {
        Payment current = getPayment(merchantId, paymentId);
        if (current.status() == PaymentStatus.AUTHORIZED) {
            return new AuthClaim(current, 0, current.pspRef(), true);
        }
        if (current.status() == PaymentStatus.AUTHORIZING) {
            int existingAttempt = countAttempts(paymentId) + 1;
            String ref = current.pspRef() != null ? current.pspRef() : paymentId + ":" + existingAttempt;
            return new AuthClaim(current, existingAttempt, ref, false);
        }
        current.status().requireTransitionTo(paymentId, PaymentStatus.AUTHORIZING, "authorize");

        int attemptNo = countAttempts(paymentId) + 1;
        String pspRef = paymentId + ":" + attemptNo;
        Instant now = clockProvider.now();
        int updated = jdbcClient.sql("""
                UPDATE payments
                SET status = 'AUTHORIZING', psp_ref = :pspRef,
                    version = version + 1, updated_at = :updatedAt
                WHERE id = :id AND merchant_id = :merchantId AND status = 'CREATED'
                """)
                .param("pspRef", pspRef)
                .param("updatedAt", Timestamp.from(now))
                .param("id", paymentId)
                .param("merchantId", merchantId)
                .update();

        if (updated == 0) {
            Payment reloaded = getPayment(merchantId, paymentId);
            if (reloaded.status() == PaymentStatus.AUTHORIZED) {
                return new AuthClaim(reloaded, attemptNo, reloaded.pspRef(), true);
            }
            throw PayCoreException.invalidStateTransition(
                    paymentId.toString(), reloaded.status().name(), "authorize");
        }
        return new AuthClaim(getPayment(merchantId, paymentId), attemptNo, pspRef, false);
    }

    private Payment finalizeAuthorization(
            UUID merchantId, UUID paymentId, AuthClaim claim, PspClient.PspAuthResult result) {
        Instant now = clockProvider.now();
        Timestamp nowTs = Timestamp.from(now);
        String outcome = switch (result.outcome()) {
            case AUTHORIZED -> "AUTHORIZED";
            case DECLINED -> "DECLINED";
            case UNKNOWN_TIMEOUT -> "TIMEOUT";
        };
        insertAttemptRow(paymentId, claim.attemptNo(), claim.pspRef(), outcome, result.latencyMs(), nowTs);

        if (result.outcome() == PspClient.PspOutcome.UNKNOWN_TIMEOUT) {
            return getPayment(merchantId, paymentId);
        }

        String nextStatus = result.outcome() == PspClient.PspOutcome.AUTHORIZED ? "AUTHORIZED" : "FAILED";
        int updated = jdbcClient.sql("""
                UPDATE payments
                SET status = :nextStatus, version = version + 1,
                    event_seq = event_seq + 1, updated_at = :updatedAt
                WHERE id = :id AND merchant_id = :merchantId AND status = 'AUTHORIZING'
                """)
                .param("nextStatus", nextStatus)
                .param("updatedAt", nowTs)
                .param("id", paymentId)
                .param("merchantId", merchantId)
                .update();

        Payment after = getPayment(merchantId, paymentId);
        if (updated == 1 && result.outcome() == PspClient.PspOutcome.AUTHORIZED) {
            outboxWriter.writePaymentAuthorized(
                    paymentId, merchantId, after.eventSeq(), after.amount(), after.currency(), after.pspRef());
        } else if (updated == 1) {
            outboxWriter.writePaymentFailed(
                    paymentId, merchantId, after.eventSeq(), after.amount(), after.currency(),
                    after.pspRef(), result.detail());
        }
        return after;
    }

    private Payment getPayment(UUID merchantId, UUID paymentId) {
        return paymentRepository.findByMerchantAndId(merchantId, paymentId);
    }

    private int countAttempts(UUID paymentId) {
        Integer count = jdbcClient.sql("SELECT COUNT(*) FROM payment_attempts WHERE payment_id = :paymentId")
                .param("paymentId", paymentId)
                .query(Integer.class)
                .single();
        return count != null ? count : 0;
    }

    private void insertAttemptRow(
            UUID paymentId, int attemptNo, String pspRef, String outcome, long latencyMs, Timestamp createdAt) {
        jdbcClient.sql("""
                INSERT INTO payment_attempts (id, payment_id, attempt_no, psp_ref, outcome, latency_ms, created_at)
                VALUES (:id, :paymentId, :attemptNo, :pspRef, :outcome, :latencyMs, :createdAt)
                ON CONFLICT (payment_id, attempt_no) DO UPDATE
                SET outcome = EXCLUDED.outcome, latency_ms = EXCLUDED.latency_ms
                """)
                .param("id", UuidV7.generate(createdAt.getTime()))
                .param("paymentId", paymentId)
                .param("attemptNo", attemptNo)
                .param("pspRef", pspRef)
                .param("outcome", outcome)
                .param("latencyMs", latencyMs)
                .param("createdAt", createdAt)
                .update();
    }

    void setCrashHook(Consumer<String> crashHook) {
        this.crashHook = crashHook != null ? crashHook : stage -> {};
    }

    private record AuthClaim(Payment payment, int attemptNo, String pspRef, boolean alreadyAuthorized) {}
}
