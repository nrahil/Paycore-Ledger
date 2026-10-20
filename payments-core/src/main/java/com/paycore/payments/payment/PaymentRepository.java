package com.paycore.payments.payment;

import com.paycore.common.errors.PayCoreException;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Owns merchant-scoped payment reads and row locking for lifecycle transactions. */
@Repository
public class PaymentRepository {

    private static final String PAYMENT_COLUMNS = """
            id, merchant_id, amount, currency, status, captured_amount, refunded_amount,
            psp_ref, metadata::text AS metadata_json, version, event_seq, created_at, updated_at
            """;

    private final JdbcClient jdbcClient;
    private final PaymentRowMapper rowMapper;

    public PaymentRepository(JdbcClient jdbcClient, PaymentRowMapper rowMapper) {
        this.jdbcClient = jdbcClient;
        this.rowMapper = rowMapper;
    }

    Payment findByMerchantAndId(UUID merchantId, UUID paymentId) {
        return query(merchantId, paymentId, false);
    }

    Payment lockByMerchantAndId(UUID merchantId, UUID paymentId) {
        return query(merchantId, paymentId, true);
    }

    private Payment query(UUID merchantId, UUID paymentId, boolean lock) {
        String sql = "SELECT " + PAYMENT_COLUMNS + " FROM payments "
                + "WHERE id = :id AND merchant_id = :merchantId"
                + (lock ? " FOR UPDATE" : "");
        return jdbcClient.sql(sql)
                .param("id", paymentId)
                .param("merchantId", merchantId)
                .query(rowMapper)
                .optional()
                .orElseThrow(() -> PayCoreException.notFound("Payment", paymentId));
    }
}
