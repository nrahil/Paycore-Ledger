package com.paycore.readmodel;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Serves read-only payment and merchant-summary queries from the CQRS projections. */
@Service
public class ReadModelQueryService {

    private final JdbcClient jdbcClient;

    public ReadModelQueryService(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    @Transactional(readOnly = true)
    public ReadModelViews.CursorPage<ReadModelViews.PaymentViewRow> listPayments(
            UUID merchantId, UUID cursor, int limit) {
        int clampedLimit = Math.max(1, Math.min(limit, 100));
        List<ReadModelViews.PaymentViewRow> rows;
        if (cursor == null) {
            rows = jdbcClient.sql("""
                    SELECT payment_id, merchant_id, status, amount, captured_amount,
                           refunded_amount, fee_amount, currency, psp_ref, last_event_seq, updated_at
                    FROM payment_view
                    WHERE merchant_id = :merchantId
                    ORDER BY payment_id DESC
                    LIMIT :limitPlusOne
                    """)
                    .param("merchantId", merchantId)
                    .param("limitPlusOne", clampedLimit + 1)
                    .query(ReadModelQueryService::mapPaymentView)
                    .list();
        } else {
            rows = jdbcClient.sql("""
                    SELECT payment_id, merchant_id, status, amount, captured_amount,
                           refunded_amount, fee_amount, currency, psp_ref, last_event_seq, updated_at
                    FROM payment_view
                    WHERE merchant_id = :merchantId
                      AND payment_id < :cursor
                    ORDER BY payment_id DESC
                    LIMIT :limitPlusOne
                    """)
                    .param("merchantId", merchantId)
                    .param("cursor", cursor)
                    .param("limitPlusOne", clampedLimit + 1)
                    .query(ReadModelQueryService::mapPaymentView)
                    .list();
        }

        boolean hasMore = rows.size() > clampedLimit;
        List<ReadModelViews.PaymentViewRow> items = hasMore ? rows.subList(0, clampedLimit) : rows;
        UUID nextCursor = hasMore ? items.get(items.size() - 1).paymentId() : null;
        return new ReadModelViews.CursorPage<>(items, nextCursor, hasMore);
    }

    @Transactional(readOnly = true)
    public ReadModelViews.MerchantSummaryResponse getMerchantSummary(UUID merchantId) {
        record Totals(String currency, long captured, long refunded, long fees, long paymentCount) {}
        Map<String, Totals> paymentTotals = jdbcClient.sql("""
                SELECT
                    currency,
                    COALESCE(SUM(captured_amount), 0) AS total_captured,
                    COALESCE(SUM(refunded_amount), 0) AS total_refunded,
                    COALESCE(SUM(fee_amount), 0)      AS total_fees,
                    COUNT(*) FILTER (WHERE captured_amount > 0) AS payment_count
                FROM payment_view
                WHERE merchant_id = :merchantId
                GROUP BY currency
                """)
                .param("merchantId", merchantId)
                .query((rs, rowNum) -> new Totals(
                        rs.getString("currency"), rs.getLong("total_captured"),
                        rs.getLong("total_refunded"), rs.getLong("total_fees"),
                        rs.getLong("payment_count")))
                .list().stream().collect(java.util.stream.Collectors.toMap(Totals::currency, totals -> totals));

        List<ReadModelViews.DailySummaryRow> daily = jdbcClient.sql("""
                SELECT merchant_id, day, currency, captured, refunded, fees, payout_amount, payment_count
                FROM merchant_daily_summary
                WHERE merchant_id = :merchantId
                ORDER BY currency, day DESC
                """)
                .param("merchantId", merchantId)
                .query((rs, rowNum) -> new ReadModelViews.DailySummaryRow(
                        rs.getObject("merchant_id", UUID.class),
                        rs.getDate("day").toLocalDate(),
                        rs.getString("currency"),
                        rs.getLong("captured"),
                        rs.getLong("refunded"),
                        rs.getLong("fees"),
                        rs.getLong("payout_amount"),
                        rs.getLong("payment_count")))
                .list();

        java.util.Set<String> currencies = new java.util.TreeSet<>(paymentTotals.keySet());
        daily.forEach(row -> currencies.add(row.currency()));
        List<ReadModelViews.CurrencySummary> summaries = currencies.stream().map(currency -> {
            Totals totals = paymentTotals.getOrDefault(currency, new Totals(currency, 0, 0, 0, 0));
            List<ReadModelViews.DailySummaryRow> currencyDaily = daily.stream()
                    .filter(row -> row.currency().equals(currency)).toList();
            long payouts = currencyDaily.stream().mapToLong(ReadModelViews.DailySummaryRow::payoutAmount).sum();
            return new ReadModelViews.CurrencySummary(currency, totals.captured(), totals.refunded(), totals.fees(),
                    payouts, totals.captured() - totals.refunded() - totals.fees() - payouts,
                    totals.paymentCount(), currencyDaily);
        }).toList();
        return new ReadModelViews.MerchantSummaryResponse(merchantId, summaries);
    }

    private static ReadModelViews.PaymentViewRow mapPaymentView(ResultSet rs, int rowNum) throws SQLException {
        return new ReadModelViews.PaymentViewRow(
                rs.getObject("payment_id", UUID.class),
                rs.getObject("merchant_id", UUID.class),
                rs.getString("status"),
                rs.getLong("amount"),
                rs.getLong("captured_amount"),
                rs.getLong("refunded_amount"),
                rs.getLong("fee_amount"),
                rs.getString("currency"),
                rs.getString("psp_ref"),
                rs.getLong("last_event_seq"),
                rs.getTimestamp("updated_at").toInstant());
    }
}
