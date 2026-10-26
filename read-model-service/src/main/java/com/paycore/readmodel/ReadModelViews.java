package com.paycore.readmodel;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/** API views for projected payment, merchant-summary, and rebuild queries. */
public final class ReadModelViews {

    private ReadModelViews() {}

    public record PaymentViewRow(
            UUID paymentId,
            UUID merchantId,
            String status,
            long amount,
            long capturedAmount,
            long refundedAmount,
            long feeAmount,
            String currency,
            String pspRef,
            long lastEventSeq,
            Instant updatedAt) {}

    public record CursorPage<T>(List<T> items, UUID nextCursor, boolean hasMore) {}

    public record DailySummaryRow(
            UUID merchantId,
            LocalDate day,
            String currency,
            long captured,
            long refunded,
            long fees,
            long payoutAmount,
            long paymentCount) {}

    public record CurrencySummary(
            String currency,
            long totalCaptured,
            long totalRefunded,
            long totalFees,
            long totalPayouts,
            long netMerchantAmount,
            long paymentCount,
            List<DailySummaryRow> dailyBreakdown) {}

    public record MerchantSummaryResponse(UUID merchantId, List<CurrencySummary> currencies) {}

    public record RebuildReport(
            long eventsProcessed,
            long paymentsProjected,
            long totalCaptured,
            long totalRefunded,
            long totalFees,
            Instant rebuiltAt) {}

    public record DeadLetterView(
            UUID id,
            String sourceTopic,
            int sourcePartition,
            long sourceOffset,
            String messageKey,
            String exceptionClass,
            String exceptionMessage,
            Instant failedAt,
            int replayCount,
            Instant lastReplayedAt) {}

    public record DeadLetterReplayResult(UUID id, String sourceTopic, int replayCount, Instant replayedAt) {}
}
