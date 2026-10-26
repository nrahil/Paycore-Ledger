package com.paycore.webhook;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.paycore.common.events.v1.PaymentEvent;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.stereotype.Component;

/** Maps versioned payment events into the stable JSON body sent to merchant endpoints. */
@Component
public class WebhookPayloadMapper {

    private final ObjectMapper objectMapper;

    public WebhookPayloadMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String toJson(PaymentEvent event) {
        Map<String, Object> root = new LinkedHashMap<>();
        root.put("event_id", event.getEventId());
        root.put("event_type", event.getEventType());
        root.put("schema_version", event.getSchemaVersion());
        root.put("payment_id", event.getPaymentId());
        root.put("merchant_id", event.getMerchantId());
        root.put("event_seq", event.getEventSeq());
        root.put("occurred_at", Instant.ofEpochSecond(
                event.getOccurredAt().getSeconds(), event.getOccurredAt().getNanos()).toString());

        Map<String, Object> data = new LinkedHashMap<>();
        switch (event.getPayloadCase()) {
            case AUTHORIZED -> {
                data.put("amount", event.getAuthorized().getAmount());
                data.put("currency", event.getAuthorized().getCurrency());
                data.put("psp_ref", event.getAuthorized().getPspRef());
            }
            case CAPTURED -> {
                data.put("captured_amount", event.getCaptured().getCapturedAmount());
                data.put("fee_amount", event.getCaptured().getFeeAmount());
                data.put("net_merchant_amount", event.getCaptured().getNetMerchantAmount());
                data.put("currency", event.getCaptured().getCurrency());
                data.put("psp_ref", event.getCaptured().getPspRef());
                data.put("ledger_txn_id", event.getCaptured().getLedgerTxnId());
            }
            case REFUNDED -> {
                data.put("refund_id", event.getRefunded().getRefundId());
                data.put("refund_amount", event.getRefunded().getRefundAmount());
                data.put("fee_reversed_amount", event.getRefunded().getFeeReversedAmount());
                data.put("total_refunded_amount", event.getRefunded().getTotalRefundedAmount());
                data.put("currency", event.getRefunded().getCurrency());
                data.put("ledger_txn_id", event.getRefunded().getLedgerTxnId());
            }
            case PAYOUT -> {
                data.put("payout_id", event.getPayout().getPayoutId());
                data.put("amount", event.getPayout().getAmount());
                data.put("currency", event.getPayout().getCurrency());
                data.put("ledger_txn_id", event.getPayout().getLedgerTxnId());
            }
            case FAILED -> {
                data.put("amount", event.getFailed().getAmount());
                data.put("currency", event.getFailed().getCurrency());
                data.put("psp_ref", event.getFailed().getPspRef());
                data.put("reason", event.getFailed().getReason());
            }
            default -> {}
        }
        root.put("data", data);

        try {
            return objectMapper.writeValueAsString(root);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize webhook JSON", e);
        }
    }
}
