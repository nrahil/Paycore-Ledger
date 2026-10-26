package com.paycore.payments.payment;

import com.paycore.common.clock.ClockProvider;
import com.paycore.common.errors.PayCoreException;
import com.paycore.common.ids.UuidV7;
import com.paycore.common.money.CurrencyCode;
import com.paycore.common.money.Money;
import com.paycore.payments.ledger.LedgerService;
import com.paycore.payments.ledger.LedgerTransaction;
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

/** Coordinates durable capture/refund reservations, PSP calls, and atomic local finalization. */
@Service
public class PaymentCaptureRefundService {

    private final JdbcClient jdbcClient;
    private final LedgerService ledgerService;
    private final OutboxWriter outboxWriter;
    private final PspClient pspClient;
    private final PaymentRepository paymentRepository;
    private final ClockProvider clockProvider;
    private final TransactionTemplate requiresNewTx;
    private volatile Consumer<String> crashHook = stage -> {};

    public PaymentCaptureRefundService(
            JdbcClient jdbcClient,
            LedgerService ledgerService,
            OutboxWriter outboxWriter,
            PspClient pspClient,
            PaymentRepository paymentRepository,
            ClockProvider clockProvider,
            PlatformTransactionManager txManager) {
        this.jdbcClient = jdbcClient;
        this.ledgerService = ledgerService;
        this.outboxWriter = outboxWriter;
        this.pspClient = pspClient;
        this.paymentRepository = paymentRepository;
        this.clockProvider = clockProvider;
        this.requiresNewTx = new TransactionTemplate(txManager);
        this.requiresNewTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public Payment capturePayment(UUID merchantId, UUID paymentId, int merchantFeeBps) {
        CaptureClaim claim = requiresNewTx.execute(status -> reserveCapture(merchantId, paymentId, merchantFeeBps));

        crashHook.accept("AFTER_TX1_BEFORE_PSP");

        PspClient.PspCaptureResult pspResult;
        try {
            pspResult = pspClient.capture(new PspClient.PspCaptureRequest(
                    paymentId, merchantId, claim.operationId(), claim.payment().pspRef(), claim.payment().amount(), claim.payment().currency()));
        } catch (RuntimeException ex) {
            throw pspFailure("capture", paymentId, ex);
        }

        crashHook.accept("AFTER_PSP_BEFORE_TX2");

        CaptureCompletion completion = requiresNewTx.execute(status -> finalizeCapture(
                merchantId, paymentId, claim.operationId(), pspResult));
        if (!completion.success()) {
            throw pspFailure("capture", paymentId, completion.detail());
        }
        return completion.payment();
    }

    private CaptureClaim reserveCapture(UUID merchantId, UUID paymentId, int merchantFeeBps) {
        Payment current = lockPaymentRow(merchantId, paymentId);
        if (current.status() == PaymentStatus.CAPTURED || current.status() == PaymentStatus.CAPTURING) {
            throw PayCoreException.invalidStateTransition(paymentId.toString(), current.status().name(), "capture");
        }
        current.status().requireTransitionTo(paymentId, PaymentStatus.CAPTURING, "capture");

        UUID operationId = UuidV7.generate(clockProvider.now().toEpochMilli());
        Instant now = clockProvider.now();
        Money gross = Money.ofPositive(current.amount(), CurrencyCode.fromString(current.currency()));
        long feeAmount = gross.calculateFeeBps(merchantFeeBps).amountMinor();
        insertOperation(operationId, merchantId, paymentId, "CAPTURE", current.amount(), feeAmount, now);
        int updated = jdbcClient.sql("""
                UPDATE payments
                SET status = 'CAPTURING',
                    version = version + 1,
                    updated_at = :updatedAt
                WHERE id = :id
                  AND merchant_id = :merchantId
                  AND status = 'AUTHORIZED'
                """)
                .param("updatedAt", Timestamp.from(now))
                .param("id", paymentId)
                .param("merchantId", merchantId)
                .update();
        if (updated != 1) {
            throw PayCoreException.invalidStateTransition(paymentId.toString(), current.status().name(), "capture");
        }
        return new CaptureClaim(operationId, getPayment(merchantId, paymentId));
    }

    CaptureCompletion finalizeCapture(
            UUID merchantId,
            UUID paymentId,
            UUID operationId,
            PspClient.PspCaptureResult pspResult) {
        if (pspResult.outcome() != PspClient.PspOutcome.AUTHORIZED) {
            if (pspResult.outcome() == PspClient.PspOutcome.DECLINED) {
                markOperationFailed(operationId);
                jdbcClient.sql("""
                        UPDATE payments
                        SET status = 'AUTHORIZED', version = version + 1, updated_at = :updatedAt
                        WHERE id = :id AND merchant_id = :merchantId AND status = 'CAPTURING'
                        """)
                        .param("updatedAt", Timestamp.from(clockProvider.now()))
                        .param("id", paymentId)
                        .param("merchantId", merchantId)
                        .update();
            }
            return new CaptureCompletion(false, getPayment(merchantId, paymentId), pspResult.detail());
        }

        Payment locked = lockPaymentRow(merchantId, paymentId);
        if (!isOperationInProgress(operationId)) {
            return new CaptureCompletion(true, getPayment(merchantId, paymentId), "ALREADY_FINALIZED");
        }
        Money grossAmount = Money.ofPositive(locked.amount(), CurrencyCode.fromString(locked.currency()));
        Money feeAmount = Money.ofNonNegative(getOperationFee(operationId), grossAmount.currency());

        UUID ledgerTxnId = ledgerService.post(
                LedgerTransaction.forCapture(paymentId, merchantId, grossAmount, feeAmount, "payments-core:capture"));

        crashHook.accept("MID_TX3_AFTER_LEDGER");

        Instant now = clockProvider.now();
        long nextEventSeq = locked.eventSeq() + 1;

        int updated = jdbcClient.sql("""
                UPDATE payments
                SET status = 'CAPTURED',
                    captured_amount = amount,
                    version = version + 1,
                    event_seq = :eventSeq,
                    updated_at = :updatedAt
                WHERE id = :id
                  AND merchant_id = :merchantId
                  AND status = 'CAPTURING'
                """)
                .param("eventSeq", nextEventSeq)
                .param("updatedAt", Timestamp.from(now))
                .param("id", paymentId)
                .param("merchantId", merchantId)
                .update();

        if (updated != 1) {
            throw PayCoreException.invalidStateTransition(paymentId.toString(), locked.status().name(), "capture");
        }
        markOperationCompleted(operationId);

        outboxWriter.writePaymentCaptured(
                paymentId,
                merchantId,
                nextEventSeq,
                grossAmount.amountMinor(),
                feeAmount.amountMinor(),
                grossAmount.subtract(feeAmount).amountMinor(),
                locked.currency(),
                locked.pspRef(),
                ledgerTxnId);

        return new CaptureCompletion(true, getPayment(merchantId, paymentId), "CAPTURED");
    }

    public PaymentService.RefundResponse refundPayment(UUID merchantId, UUID paymentId, PaymentService.RefundRequest request) {
        if (request.amount() <= 0) {
            throw PayCoreException.unprocessable("INVALID_REFUND_AMOUNT", "Invalid Refund Amount", "Refund amount must be > 0");
        }

        RefundClaim claim = requiresNewTx.execute(status -> reserveRefund(merchantId, paymentId, request));
        Payment payment = claim.payment();

        crashHook.accept("AFTER_TX1_BEFORE_PSP");
        PspClient.PspRefundResult pspResult;
        try {
            pspResult = pspClient.refund(new PspClient.PspRefundRequest(
                    paymentId, merchantId, claim.operationId(), payment.pspRef(), request.amount(), payment.currency()));
        } catch (RuntimeException ex) {
            throw pspFailure("refund", paymentId, ex);
        }

        crashHook.accept("AFTER_PSP_BEFORE_TX2");
        RefundCompletion completion = requiresNewTx.execute(status -> finalizeRefund(
                merchantId, paymentId, claim.operationId(), request, pspResult));
        if (!completion.success()) {
            throw pspFailure("refund", paymentId, completion.detail());
        }
        return completion.response();
    }

    private RefundClaim reserveRefund(UUID merchantId, UUID paymentId, PaymentService.RefundRequest request) {
        Payment payment = lockPaymentRow(merchantId, paymentId);
        if (payment.status() != PaymentStatus.CAPTURED
                && payment.status() != PaymentStatus.PARTIALLY_REFUNDED) {
            throw PayCoreException.invalidStateTransition(paymentId.toString(), payment.status().name(), "refund");
        }

        Long inFlight = jdbcClient.sql("""
                        SELECT COALESCE(SUM(amount), 0)
                        FROM payment_operations
                        WHERE payment_id = :paymentId AND operation_type = 'REFUND' AND status = 'IN_PROGRESS'
                        """)
                .param("paymentId", paymentId)
                .query(Long.class)
                .single();
        long newReservedTotal = Math.addExact(Math.addExact(payment.refundedAmount(), inFlight), request.amount());
        if (newReservedTotal > payment.capturedAmount()) {
            throw PayCoreException.unprocessable(
                    "REFUND_EXCEEDS_CAPTURED_AMOUNT",
                    "Refund Exceeds Captured Amount",
                    "Requested refund " + request.amount() + " plus existing refunded "
                            + payment.refundedAmount() + " exceeds captured amount " + payment.capturedAmount());
        }

        UUID operationId = UuidV7.generate(clockProvider.now().toEpochMilli());
        long capturedFee = jdbcClient.sql("""
                SELECT fee_amount FROM payment_operations
                WHERE payment_id = :paymentId AND operation_type = 'CAPTURE' AND status = 'COMPLETED'
                ORDER BY updated_at DESC LIMIT 1
                """)
                .param("paymentId", paymentId).query(Long.class).single();
        Long priorRefundFees = jdbcClient.sql("""
                SELECT COALESCE(SUM(fee_amount), 0) FROM payment_operations
                WHERE payment_id = :paymentId AND operation_type = 'REFUND' AND status IN ('IN_PROGRESS', 'COMPLETED')
                """)
                .param("paymentId", paymentId).query(Long.class).single();
        long newRefundedTotal = Math.addExact(Math.addExact(payment.refundedAmount(), inFlight), request.amount());
        long cappedRefundedTotal = Math.min(newRefundedTotal, payment.capturedAmount());
        long targetFeeReversal = cappedRefundedTotal == payment.capturedAmount()
                ? capturedFee
                : java.math.BigInteger.valueOf(capturedFee).multiply(java.math.BigInteger.valueOf(cappedRefundedTotal))
                        .divide(java.math.BigInteger.valueOf(payment.capturedAmount())).longValueExact();
        long feeReversal = Math.max(0L, Math.subtractExact(targetFeeReversal, priorRefundFees));
        insertOperation(operationId, merchantId, paymentId, "REFUND", request.amount(), feeReversal, clockProvider.now());
        return new RefundClaim(operationId, payment);
    }

    RefundCompletion finalizeRefund(
            UUID merchantId,
            UUID paymentId,
            UUID operationId,
            PaymentService.RefundRequest request,
            PspClient.PspRefundResult pspResult) {
        if (pspResult.outcome() != PspClient.PspOutcome.AUTHORIZED) {
            if (pspResult.outcome() == PspClient.PspOutcome.DECLINED) {
                markOperationFailed(operationId);
            }
            return new RefundCompletion(false, null, pspResult.detail());
        }

        Payment payment = lockPaymentRow(merchantId, paymentId);
        if (!isOperationInProgress(operationId)) {
            return new RefundCompletion(false, null, "ALREADY_FINALIZED");
        }
        Money refundMoney = Money.ofPositive(request.amount(), CurrencyCode.fromString(payment.currency()));
        long feeReversal = getOperationFee(operationId);
        UUID ledgerTxnId = ledgerService.post(
                LedgerTransaction.forRefund(paymentId, merchantId, refundMoney,
                        Money.ofNonNegative(feeReversal, refundMoney.currency()), "payments-core:refund"));

        Instant now = clockProvider.now();
        int updatedRows = jdbcClient.sql("""
                UPDATE payments
                SET status = CASE WHEN refunded_amount + :refundAmount >= captured_amount THEN 'REFUNDED' ELSE 'PARTIALLY_REFUNDED' END,
                    refunded_amount = refunded_amount + :refundAmount,
                    version = version + 1,
                    event_seq = event_seq + 1,
                    updated_at = :updatedAt
                WHERE id = :id AND merchant_id = :merchantId
                """)
                .param("refundAmount", request.amount())
                .param("updatedAt", Timestamp.from(now))
                .param("id", paymentId)
                .param("merchantId", merchantId)
                .update();
        if (updatedRows != 1) {
            throw PayCoreException.notFound("Payment", paymentId);
        }
        markOperationCompleted(operationId);

        Payment updated = getPayment(merchantId, paymentId);
        outboxWriter.writeRefundProcessed(
                paymentId, merchantId, updated.eventSeq(), operationId, request.amount(), feeReversal, updated.refundedAmount(),
                updated.currency(), ledgerTxnId);

        return new RefundCompletion(
                true,
                new PaymentService.RefundResponse(operationId, paymentId, merchantId, request.amount(), updated.refundedAmount(),
                        updated.capturedAmount(), updated.currency(), feeReversal, ledgerTxnId, now),
                "REFUNDED");
    }

    private void insertOperation(
            UUID operationId, UUID merchantId, UUID paymentId, String operationType, long amount, long feeAmount, Instant now) {
        jdbcClient.sql("""
                INSERT INTO payment_operations (
                    id, merchant_id, payment_id, operation_type, amount, fee_amount, status, created_at, updated_at
                ) VALUES (:id, :merchantId, :paymentId, :operationType, :amount, :feeAmount, 'IN_PROGRESS', :createdAt, :updatedAt)
                """)
                .param("id", operationId)
                .param("merchantId", merchantId)
                .param("paymentId", paymentId)
                .param("operationType", operationType)
                .param("amount", amount)
                .param("feeAmount", feeAmount)
                .param("createdAt", Timestamp.from(now))
                .param("updatedAt", Timestamp.from(now))
                .update();
    }

    private Payment getPayment(UUID merchantId, UUID paymentId) {
        return paymentRepository.findByMerchantAndId(merchantId, paymentId);
    }

    private Payment lockPaymentRow(UUID merchantId, UUID paymentId) {
        return paymentRepository.lockByMerchantAndId(merchantId, paymentId);
    }

    private long getOperationFee(UUID operationId) {
        return jdbcClient.sql("SELECT fee_amount FROM payment_operations WHERE id = :id")
                .param("id", operationId).query(Long.class).single();
    }

    private boolean isOperationInProgress(UUID operationId) {
        Boolean inProgress = jdbcClient.sql("SELECT status = 'IN_PROGRESS' FROM payment_operations WHERE id = :id")
                .param("id", operationId).query(Boolean.class).optional().orElse(false);
        return Boolean.TRUE.equals(inProgress);
    }

    private UUID findInProgressOperation(UUID paymentId, String operationType) {
        return jdbcClient.sql("""
                SELECT id
                FROM payment_operations
                WHERE payment_id = :paymentId AND operation_type = :operationType AND status = 'IN_PROGRESS'
                ORDER BY created_at DESC
                LIMIT 1
                """)
                .param("paymentId", paymentId)
                .param("operationType", operationType)
                .query(UUID.class)
                .optional()
                .orElse(null);
    }

    private void markOperationCompleted(UUID operationId) {
        jdbcClient.sql("""
                UPDATE payment_operations
                SET status = 'COMPLETED', updated_at = :updatedAt
                WHERE id = :id AND status = 'IN_PROGRESS'
                """)
                .param("id", operationId)
                .param("updatedAt", Timestamp.from(clockProvider.now()))
                .update();
    }

    private void markOperationFailed(UUID operationId) {
        jdbcClient.sql("""
                UPDATE payment_operations
                SET status = 'FAILED', updated_at = :updatedAt
                WHERE id = :id AND status = 'IN_PROGRESS'
                """)
                .param("id", operationId)
                .param("updatedAt", Timestamp.from(clockProvider.now()))
                .update();
    }

    private PayCoreException pspFailure(String operation, UUID paymentId, String detail) {
        return new PayCoreException(
                502,
                "PSP_OPERATION_FAILED",
                "PSP Operation Failed",
                "PSP " + operation + " failed for payment " + paymentId + ": " + detail);
    }

    private PayCoreException pspFailure(String operation, UUID paymentId, Throwable cause) {
        String detail = cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
        return new PayCoreException(
                502,
                "PSP_OPERATION_FAILED",
                "PSP Operation Failed",
                "PSP " + operation + " failed for payment " + paymentId + ": " + detail,
                cause);
    }

    void setCrashHook(Consumer<String> crashHook) {
        this.crashHook = crashHook != null ? crashHook : stage -> {};
    }

    private record CaptureClaim(UUID operationId, Payment payment) {}
    record CaptureCompletion(boolean success, Payment payment, String detail) {}
    private record RefundClaim(UUID operationId, Payment payment) {}
    record RefundCompletion(boolean success, PaymentService.RefundResponse response, String detail) {}
}
