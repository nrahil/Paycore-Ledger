package com.paycore.payments.payment;

import com.paycore.common.clock.ClockProvider;
import com.paycore.common.errors.PayCoreException;
import com.paycore.common.ids.UuidV7;
import com.paycore.common.money.CurrencyCode;
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
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

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
    private final PaymentRowMapper paymentRowMapper;
    private final PaymentRepository paymentRepository;
    private final PaymentAuthorizationService paymentAuthorizationService;
    private final ClockProvider clockProvider;
    private final PaymentRecoveryService paymentRecoveryService;
    private final PayoutService payoutService;

    private final PaymentCaptureRefundService captureRefundService;

    public PaymentService(
            JdbcClient jdbcClient,
            PaymentRowMapper paymentRowMapper,
            PaymentRepository paymentRepository,
            PaymentAuthorizationService paymentAuthorizationService,
            PaymentCaptureRefundService captureRefundService,
            PaymentRecoveryService paymentRecoveryService,
            PayoutService payoutService,
            ClockProvider clockProvider) {
        this.jdbcClient = jdbcClient;
        this.paymentRowMapper = paymentRowMapper;
        this.paymentRepository = paymentRepository;
        this.paymentAuthorizationService = paymentAuthorizationService;
        this.captureRefundService = captureRefundService;
        this.paymentRecoveryService = paymentRecoveryService;
        this.payoutService = payoutService;
        this.clockProvider = clockProvider;
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
        return captureRefundService.capturePayment(merchantId, paymentId, merchantFeeBps);
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
        return captureRefundService.refundPayment(merchantId, paymentId, request);
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
    public PayoutResponse createPayout(UUID merchantId, PayoutRequest request) {
        return payoutService.createPayout(merchantId, request);
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

    public void setCrashHook(Consumer<String> crashHook) {
        paymentAuthorizationService.setCrashHook(crashHook);
        captureRefundService.setCrashHook(crashHook);
    }

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
