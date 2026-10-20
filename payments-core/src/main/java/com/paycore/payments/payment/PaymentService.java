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
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Core Payment Lifecycle Service (Section 4.3, 5.1, 5.3).
 *
 * <p>Implements:
 * <ul>
 *   <li>Create payment intent</li>
 *   <li>Two-transaction Authorize across external PSP call (never holding a DB connection during network I/O)</li>
 *   <li>Unknown-outcome (PSP timeout) background resolver</li>
 *   <li>Two-transaction Capture and Full/Partial Refunds across external PSP calls</li>
 *   <li>Void and final refund settlement serialized via {@code SELECT ... FOR UPDATE}</li>
 *   <li>Settlement Payouts</li>
 * </ul>
 */
@Service
public class PaymentService {

    private final JdbcClient jdbcClient;
    private final LedgerService ledgerService;
    private final OutboxWriter outboxWriter;
    private final PspClient pspClient;
    private final PaymentRowMapper paymentRowMapper;
    private final PaymentRepository paymentRepository;
    private final PaymentAuthorizationService paymentAuthorizationService;
    private final ClockProvider clockProvider;
    private final TransactionTemplate requiresNewTx;
    private final PaymentRecoveryService paymentRecoveryService;

    private volatile Consumer<String> crashHook = stage -> {};

    public PaymentService(
            JdbcClient jdbcClient,
            LedgerService ledgerService,
            OutboxWriter outboxWriter,
            PspClient pspClient,
            PaymentRowMapper paymentRowMapper,
            PaymentRepository paymentRepository,
            PaymentAuthorizationService paymentAuthorizationService,
            ClockProvider clockProvider,
            PlatformTransactionManager txManager) {
        this.jdbcClient = jdbcClient;
        this.ledgerService = ledgerService;
        this.outboxWriter = outboxWriter;
        this.pspClient = pspClient;
        this.paymentRowMapper = paymentRowMapper;
        this.paymentRepository = paymentRepository;
        this.paymentAuthorizationService = paymentAuthorizationService;
        this.clockProvider = clockProvider;
        this.requiresNewTx = new TransactionTemplate(txManager);
        this.requiresNewTx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        this.paymentRecoveryService = new PaymentRecoveryService(this, jdbcClient, pspClient, outboxWriter,
                clockProvider, txManager);
    }

    /**
     * Initializes a new payment intent with {@code CREATED} state in minor currency units (paise).
     *
     * @param merchantId authenticated merchant identifier owning this payment
     * @param request creation parameters including positive amount in minor units, currency, and optional metadata
     * @return persisted {@link Payment} aggregate record in {@code CREATED} state
     * @throws PayCoreException if amount is non-positive (422 INVALID_AMOUNT) or currency is unsupported
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public Payment createPayment(UUID merchantId, CreatePaymentRequest request) {
        if (request.amount() <= 0) {
            throw PayCoreException.unprocessable("INVALID_AMOUNT", "Invalid Payment Amount", "Amount must be > 0 in minor units (paise)");
        }
        CurrencyCode currency = CurrencyCode.fromString(request.currency() != null ? request.currency() : "INR");
        Instant now = clockProvider.now();
        Timestamp nowTs = Timestamp.from(now);
        UUID paymentId = UuidV7.generate(now.toEpochMilli());
        Map<String, String> metadata = request.metadata() != null ? request.metadata() : Collections.emptyMap();
        String metadataJson = paymentRowMapper.serializeMetadata(metadata);

        jdbcClient.sql("""
                INSERT INTO payments (
                    id, merchant_id, amount, currency, status,
                    captured_amount, refunded_amount, psp_ref, metadata,
                    version, event_seq, created_at, updated_at
                ) VALUES (
                    :id, :merchantId, :amount, :currency, 'CREATED',
                    0, 0, NULL, CAST(:metadata AS jsonb),
                    1, 0, :createdAt, :updatedAt
                )
                """)
                .param("id", paymentId)
                .param("merchantId", merchantId)
                .param("amount", request.amount())
                .param("currency", currency.name())
                .param("metadata", metadataJson)
                .param("createdAt", nowTs)
                .param("updatedAt", nowTs)
                .update();

        return getPayment(merchantId, paymentId);
    }

    /** Delegates authorization orchestration to its use-case service. */
    public Payment authorizePayment(UUID merchantId, UUID paymentId) {
        return paymentAuthorizationService.authorizePayment(merchantId, paymentId);
    }
    /**
     * Executes a two-phase settlement capture, posting balanced 3-leg entries to the double-entry ledger.
     *
     * <p>Phase 1 claims the payment as {@code CAPTURING} and commits before PSP I/O. The PSP call
     * then runs with no database transaction open. Phase 2 posts the ledger, advances state to
     * {@code CAPTURED}, and writes the outbox event in one short ACID commit.
     *
     * @param merchantId authenticated merchant identifier
     * @param paymentId UUIDv7 payment identifier to capture
     * @param merchantFeeBps platform commission in basis points (e.g. 200 = 2.00%)
     * @return updated {@link Payment} aggregate in {@code CAPTURED} state
     * @throws PayCoreException if payment is not in an available capture state, or the PSP rejects/fails the capture
     */
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

    /**
     * Voids an authorized payment hold before capture occurs.
     *
     * @param merchantId authenticated merchant identifier
     * @param paymentId UUIDv7 payment identifier to void
     * @return updated {@link Payment} aggregate in {@code VOIDED} state
     * @throws PayCoreException if payment is not in {@code AUTHORIZED} state (409 INVALID_STATE_TRANSITION)
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public Payment voidPayment(UUID merchantId, UUID paymentId) {
        Payment locked = lockPaymentRow(merchantId, paymentId);
        locked.status().requireTransitionTo(paymentId, PaymentStatus.VOIDED, "void");

        Instant now = clockProvider.now();
        jdbcClient.sql("""
                UPDATE payments
                SET status = 'VOIDED',
                    version = version + 1,
                    updated_at = :updatedAt
                WHERE id = :id
                  AND merchant_id = :merchantId
                """)
                .param("updatedAt", Timestamp.from(now))
                .param("id", paymentId)
                .param("merchantId", merchantId)
                .update();

        return getPayment(merchantId, paymentId);
    }

    /**
     * Executes a two-phase partial or full refund with a durable phase-1 reservation.
     *
     * <p>Phase 1 locks the payment briefly, reserves refundable headroom in {@code payment_operations},
     * and commits. The PSP call runs outside any database transaction. Phase 2 locks the payment,
     * posts reversing ledger entries ({@code DEBIT MERCHANT_PAYABLE}, {@code CREDIT PSP_RECEIVABLE}),
     * reverses the proportional platform fee, advances the refund state, and emits {@code refund.processed} in one commit.
     *
     * @param merchantId authenticated merchant identifier
     * @param paymentId UUIDv7 payment identifier to refund
     * @param request refund request containing positive minor-unit amount and optional reason
     * @return {@link RefundResponse} containing cumulative refund state and remaining refundable headroom
     * @throws PayCoreException if amount is invalid, status is not refundable, request exceeds captured total, or the PSP rejects/fails the refund
     */
    public RefundResponse refundPayment(UUID merchantId, UUID paymentId, RefundRequest request) {
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

    private RefundClaim reserveRefund(UUID merchantId, UUID paymentId, RefundRequest request) {
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
            RefundRequest request,
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
                new RefundResponse(operationId, paymentId, merchantId, request.amount(), updated.refundedAmount(),
                        updated.capturedAmount(), updated.currency(), feeReversal, ledgerTxnId, now),
                "REFUNDED");
    }

    /**
     * Executes merchant withdrawal (payout), posting double-entry entries from {@code MERCHANT_PAYABLE} to {@code BANK}.
     *
     * <p>Decrements the merchant's liability balance, creates a completed {@link PayoutResponse},
     * and inserts {@code payout.created} outbox event in the same atomic transaction.
     *
     * @param merchantId authenticated merchant identifier withdrawing funds
     * @param request payout parameters containing positive minor-unit amount and currency
     * @return {@link PayoutResponse} confirming settlement reference and linked ledger transaction ID
     * @throws PayCoreException if amount is non-positive (422 INVALID_PAYOUT_AMOUNT) or balance is insufficient
     */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public PayoutResponse createPayout(UUID merchantId, PayoutRequest request) {
        if (request.amount() <= 0) {
            throw PayCoreException.unprocessable("INVALID_PAYOUT_AMOUNT", "Invalid Payout Amount", "Payout amount must be > 0");
        }
        String currencyStr = request.currency() != null ? request.currency() : "INR";
        CurrencyCode currency = CurrencyCode.fromString(currencyStr);
        Money payoutMoney = Money.ofPositive(request.amount(), currency);

        UUID ledgerTxnId = ledgerService.post(
                LedgerTransaction.forPayout(merchantId, payoutMoney, "payments-core:payout"));

        Instant now = clockProvider.now();
        UUID payoutId = UuidV7.generate(now.toEpochMilli());

        jdbcClient.sql("""
                INSERT INTO payouts (id, merchant_id, amount, currency, status, ledger_txn_id, created_at)
                VALUES (:id, :merchantId, :amount, :currency, 'COMPLETED', :ledgerTxnId, :createdAt)
                """)
                .param("id", payoutId)
                .param("merchantId", merchantId)
                .param("amount", request.amount())
                .param("currency", currency.name())
                .param("ledgerTxnId", ledgerTxnId)
                .param("createdAt", Timestamp.from(now))
                .update();

        outboxWriter.writePayoutCreated(payoutId, merchantId, 1L, request.amount(), currency.name(), ledgerTxnId);

        return new PayoutResponse(payoutId, merchantId, request.amount(), currency.name(), "COMPLETED", ledgerTxnId, now);
    }

    /** Delegates scheduled PSP timeout recovery to the recovery coordinator. */
    @Scheduled(fixedDelayString = "${paycore.resolver.interval-ms:5000}")
    public int resolveStuckAuthorizingPayments() {
        return paymentRecoveryService.resolveStuckAuthorizingPayments(Duration.ofSeconds(2));
    }

    public int resolveStuckAuthorizingPayments(Duration threshold) {
        return paymentRecoveryService.resolveStuckAuthorizingPayments(threshold);
    }

    @Scheduled(fixedDelayString = "${paycore.resolver.interval-ms:5000}")
    public int resolveStuckCaptureRefundOperations() {
        return paymentRecoveryService.resolveStuckCaptureRefundOperations(Duration.ofSeconds(2));
    }

    public int resolveStuckCaptureRefundOperations(Duration threshold) {
        return paymentRecoveryService.resolveStuckCaptureRefundOperations(threshold);
    }

    @Transactional(readOnly = true)
    public Payment getPayment(UUID merchantId, UUID paymentId) {
        return paymentRepository.findByMerchantAndId(merchantId, paymentId);
    }

    private Payment lockPaymentRow(UUID merchantId, UUID paymentId) {
        return paymentRepository.lockByMerchantAndId(merchantId, paymentId);
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

    public void setCrashHook(Consumer<String> crashHook) {
        this.crashHook = crashHook != null ? crashHook : stage -> {};
        paymentAuthorizationService.setCrashHook(crashHook);
    }

    private record CaptureClaim(UUID operationId, Payment payment) {}

    record CaptureCompletion(boolean success, Payment payment, String detail) {}

    private record RefundClaim(UUID operationId, Payment payment) {}
    record RefundCompletion(boolean success, RefundResponse response, String detail) {}

    public record CreatePaymentRequest(long amount, String currency, Map<String, String> metadata) {}

    public record RefundRequest(long amount, String reason) {}

    public record RefundResponse(
            UUID refundId,
            UUID paymentId,
            UUID merchantId,
            long refundAmount,
            long totalRefundedAmount,
            long capturedAmount,
            String currency,
            long feeReversedAmount,
            UUID ledgerTxnId,
            Instant createdAt) {}

    public record PayoutRequest(long amount, String currency) {}

    public record PayoutResponse(
            UUID payoutId,
            UUID merchantId,
            long amount,
            String currency,
            String status,
            UUID ledgerTxnId,
            Instant createdAt) {}
}
