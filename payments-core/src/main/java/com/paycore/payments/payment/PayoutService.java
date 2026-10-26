package com.paycore.payments.payment;

import com.paycore.common.clock.ClockProvider;
import com.paycore.common.errors.PayCoreException;
import com.paycore.common.ids.UuidV7;
import com.paycore.common.money.CurrencyCode;
import com.paycore.common.money.Money;
import com.paycore.payments.ledger.LedgerService;
import com.paycore.payments.ledger.LedgerTransaction;
import com.paycore.payments.outbox.OutboxWriter;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/** Posts merchant payouts and their durable event within one local transaction. */
@Service
public class PayoutService {

    private final JdbcClient jdbcClient;
    private final LedgerService ledgerService;
    private final OutboxWriter outboxWriter;
    private final ClockProvider clockProvider;

    public PayoutService(
            JdbcClient jdbcClient,
            LedgerService ledgerService,
            OutboxWriter outboxWriter,
            ClockProvider clockProvider) {
        this.jdbcClient = jdbcClient;
        this.ledgerService = ledgerService;
        this.outboxWriter = outboxWriter;
        this.clockProvider = clockProvider;
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public PaymentService.PayoutResponse createPayout(
            UUID merchantId, PaymentService.PayoutRequest request) {
        if (request.amount() <= 0) {
            throw PayCoreException.unprocessable(
                    "INVALID_PAYOUT_AMOUNT", "Invalid Payout Amount", "Payout amount must be > 0");
        }
        CurrencyCode currency = CurrencyCode.fromString(request.currency() != null ? request.currency() : "INR");
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
        return new PaymentService.PayoutResponse(
                payoutId, merchantId, request.amount(), currency.name(), "COMPLETED", ledgerTxnId, now);
    }
}
