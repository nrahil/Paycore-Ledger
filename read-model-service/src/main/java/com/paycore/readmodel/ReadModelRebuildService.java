package com.paycore.readmodel;

import com.paycore.common.clock.ClockProvider;
import java.util.List;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/** Rebuilds the materialized read model from its durable event log. */
@Service
public class ReadModelRebuildService {

    private final JdbcClient jdbcClient;
    private final ClockProvider clockProvider;
    private final ReadModelProjectorService projectorService;

    public ReadModelRebuildService(
            JdbcClient jdbcClient, ClockProvider clockProvider, ReadModelProjectorService projectorService) {
        this.jdbcClient = jdbcClient;
        this.clockProvider = clockProvider;
        this.projectorService = projectorService;
    }

    /** Clears and rebuilds the projections atomically; event replay and projection updates share this transaction. */
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public ReadModelViews.RebuildReport rebuildFromScratch() {
        jdbcClient.sql("TRUNCATE TABLE payment_view, merchant_daily_summary, processed_events").update();
        projectorService.replayStoredEventsInOrder();

        long eventsProcessed = jdbcClient.sql(
                        "SELECT COUNT(*) FROM processed_events WHERE consumer = :consumer")
                .param("consumer", ReadModelProjectorService.CONSUMER_NAME)
                .query(Long.class)
                .single();
        long paymentsProjected = jdbcClient.sql("SELECT COUNT(*) FROM payment_view")
                .query(Long.class)
                .single();
        long totalCaptured = jdbcClient.sql("SELECT COALESCE(SUM(captured_amount), 0) FROM payment_view")
                .query(Long.class)
                .single();
        long totalRefunded = jdbcClient.sql("SELECT COALESCE(SUM(refunded_amount), 0) FROM payment_view")
                .query(Long.class)
                .single();
        long totalFees = jdbcClient.sql("SELECT COALESCE(SUM(fee_amount), 0) FROM payment_view")
                .query(Long.class)
                .single();

        return new ReadModelViews.RebuildReport(
                eventsProcessed, paymentsProjected, totalCaptured, totalRefunded, totalFees, clockProvider.now());
    }
}
