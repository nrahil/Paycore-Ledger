package com.paycore.readmodel;

import com.paycore.common.clock.ClockProvider;
import com.paycore.common.events.v1.PaymentEvent;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/** Writes ordered payment events into the payment and merchant-summary projections. */
@Component
public class ReadModelProjectionWriter {

    private final JdbcClient jdbcClient;
    private final ClockProvider clockProvider;

    public ReadModelProjectionWriter(JdbcClient jdbcClient, ClockProvider clockProvider) {
        this.jdbcClient = jdbcClient;
        this.clockProvider = clockProvider;
    }

    void apply(PaymentEvent event) {
        UUID paymentId = UUID.fromString(event.getPaymentId());
        UUID merchantId = UUID.fromString(event.getMerchantId());
        long eventSeq = event.getEventSeq();
        Instant occurredAt = event.hasOccurredAt()
                ? Instant.ofEpochSecond(event.getOccurredAt().getSeconds(), event.getOccurredAt().getNanos())
                : clockProvider.now();
        Timestamp occurredTs = Timestamp.from(occurredAt);
        LocalDate day = occurredAt.atZone(ZoneOffset.UTC).toLocalDate();

        switch (event.getPayloadCase()) {
            case AUTHORIZED -> {
                long amount = event.getAuthorized().getAmount();
                String currency = event.getAuthorized().getCurrency();
                String pspRef = event.getAuthorized().getPspRef();
                int updated = jdbcClient.sql("""
                        INSERT INTO payment_view (
                            payment_id, merchant_id, status, amount, captured_amount,
                            refunded_amount, fee_amount, currency, psp_ref, last_event_seq, updated_at
                        ) VALUES (
                            :paymentId, :merchantId, 'AUTHORIZED', :amount, 0,
                            0, 0, :currency, :pspRef, :eventSeq, :updatedAt
                        )
                        ON CONFLICT (payment_id) DO UPDATE
                        SET status = 'AUTHORIZED',
                            amount = EXCLUDED.amount,
                            psp_ref = EXCLUDED.psp_ref,
                            last_event_seq = EXCLUDED.last_event_seq,
                            updated_at = EXCLUDED.updated_at
                        WHERE payment_view.last_event_seq = :eventSeq - 1
                        """)
                        .param("paymentId", paymentId)
                        .param("merchantId", merchantId)
                        .param("amount", amount)
                        .param("currency", currency)
                        .param("pspRef", pspRef)
                        .param("eventSeq", eventSeq)
                        .param("updatedAt", occurredTs)
                        .update();
                requirePaymentProjectionUpdated(updated, paymentId, eventSeq);
            }
            case FAILED -> {
                long amount = event.getFailed().getAmount();
                String currency = event.getFailed().getCurrency();
                String pspRef = event.getFailed().getPspRef();
                int updated = jdbcClient.sql("""
                        INSERT INTO payment_view (
                            payment_id, merchant_id, status, amount, captured_amount,
                            refunded_amount, fee_amount, currency, psp_ref, last_event_seq, updated_at
                        ) VALUES (
                            :paymentId, :merchantId, 'FAILED', :amount, 0,
                            0, 0, :currency, :pspRef, :eventSeq, :updatedAt
                        )
                        ON CONFLICT (payment_id) DO UPDATE
                        SET status = 'FAILED',
                            last_event_seq = EXCLUDED.last_event_seq,
                            updated_at = EXCLUDED.updated_at
                        WHERE payment_view.last_event_seq = :eventSeq - 1
                        """)
                        .param("paymentId", paymentId)
                        .param("merchantId", merchantId)
                        .param("amount", amount)
                        .param("currency", currency)
                        .param("pspRef", pspRef)
                        .param("eventSeq", eventSeq)
                        .param("updatedAt", occurredTs)
                        .update();
                requirePaymentProjectionUpdated(updated, paymentId, eventSeq);
            }
            case CAPTURED -> {
                long capturedAmount = event.getCaptured().getCapturedAmount();
                long feeAmount = event.getCaptured().getFeeAmount();
                int updated = jdbcClient.sql("""
                        UPDATE payment_view
                        SET status = 'CAPTURED',
                            captured_amount = :capturedAmount,
                            fee_amount = :feeAmount,
                            last_event_seq = :eventSeq,
                            updated_at = :updatedAt
                        WHERE payment_id = :paymentId
                          AND last_event_seq = :eventSeq - 1
                        """)
                        .param("capturedAmount", capturedAmount)
                        .param("feeAmount", feeAmount)
                        .param("eventSeq", eventSeq)
                        .param("updatedAt", occurredTs)
                        .param("paymentId", paymentId)
                        .update();
                requirePaymentProjectionUpdated(updated, paymentId, eventSeq);

                jdbcClient.sql("""
                        INSERT INTO merchant_daily_summary (merchant_id, day, currency, captured, refunded, fees, payment_count)
                        VALUES (:merchantId, :day, :currency, :captured, 0, :fees, 1)
                        ON CONFLICT (merchant_id, day, currency) DO UPDATE
                        SET captured = merchant_daily_summary.captured + EXCLUDED.captured,
                            fees = merchant_daily_summary.fees + EXCLUDED.fees,
                            payment_count = merchant_daily_summary.payment_count + 1
                        """)
                        .param("merchantId", merchantId)
                        .param("day", Date.valueOf(day))
                        .param("currency", event.getCaptured().getCurrency())
                        .param("captured", capturedAmount)
                        .param("fees", feeAmount)
                        .update();
            }
            case REFUNDED -> {
                long refundAmount = event.getRefunded().getRefundAmount();
                long feeReversed = event.getRefunded().getFeeReversedAmount();
                long totalRefunded = event.getRefunded().getTotalRefundedAmount();
                int updated = jdbcClient.sql("""
                        UPDATE payment_view
                        SET status = CASE
                                WHEN :totalRefunded >= captured_amount THEN 'REFUNDED'
                                ELSE 'PARTIALLY_REFUNDED'
                            END,
                            refunded_amount = :totalRefunded,
                            fee_amount = GREATEST(0, fee_amount - :feeReversed),
                            last_event_seq = :eventSeq,
                            updated_at = :updatedAt
                        WHERE payment_id = :paymentId
                          AND last_event_seq = :eventSeq - 1
                        """)
                        .param("totalRefunded", totalRefunded)
                        .param("feeReversed", feeReversed)
                        .param("eventSeq", eventSeq)
                        .param("updatedAt", occurredTs)
                        .param("paymentId", paymentId)
                        .update();
                requirePaymentProjectionUpdated(updated, paymentId, eventSeq);

                jdbcClient.sql("""
                        INSERT INTO merchant_daily_summary (merchant_id, day, currency, captured, refunded, fees, payment_count)
                        VALUES (:merchantId, :day, :currency, 0, :refunded, :negativeFeeReversal, 0)
                        ON CONFLICT (merchant_id, day, currency) DO UPDATE
                        SET refunded = merchant_daily_summary.refunded + EXCLUDED.refunded,
                            fees = merchant_daily_summary.fees + EXCLUDED.fees
                        """)
                        .param("merchantId", merchantId)
                        .param("day", Date.valueOf(day))
                        .param("currency", event.getRefunded().getCurrency())
                        .param("refunded", refundAmount)
                        .param("negativeFeeReversal", -feeReversed)
                        .update();
            }
            case PAYOUT -> {
                jdbcClient.sql("""
                        INSERT INTO merchant_daily_summary (merchant_id, day, currency, payout_amount)
                        VALUES (:merchantId, :day, :currency, :amount)
                        ON CONFLICT (merchant_id, day, currency) DO UPDATE
                        SET payout_amount = merchant_daily_summary.payout_amount + EXCLUDED.payout_amount
                        """)
                        .param("merchantId", merchantId)
                        .param("day", Date.valueOf(day))
                        .param("currency", event.getPayout().getCurrency())
                        .param("amount", event.getPayout().getAmount())
                        .update();
            }
            default -> advanceSequenceForUnknownEvent(paymentId, merchantId, eventSeq, occurredTs);
        }
    }

    private void advanceSequenceForUnknownEvent(
            UUID paymentId, UUID merchantId, long eventSeq, Timestamp occurredAt) {
        int updated = jdbcClient.sql("""
                        INSERT INTO payment_view (payment_id, merchant_id, status, last_event_seq, updated_at)
                        VALUES (:paymentId, :merchantId, 'UNKNOWN', :eventSeq, :updatedAt)
                        ON CONFLICT (payment_id) DO UPDATE
                        SET last_event_seq = EXCLUDED.last_event_seq,
                            updated_at = EXCLUDED.updated_at
                        WHERE payment_view.last_event_seq = :eventSeq - 1
                        """)
                .param("paymentId", paymentId)
                .param("merchantId", merchantId)
                .param("eventSeq", eventSeq)
                .param("updatedAt", occurredAt)
                .update();
        requirePaymentProjectionUpdated(updated, paymentId, eventSeq);
    }

    private void requirePaymentProjectionUpdated(int updated, UUID paymentId, long eventSeq) {
        if (updated != 1) {
            throw new IllegalStateException(
                    "Expected one payment projection update for " + paymentId + " at event_seq " + eventSeq
                            + ", but updated " + updated);
        }
    }
}
