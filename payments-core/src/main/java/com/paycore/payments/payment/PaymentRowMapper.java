package com.paycore.payments.payment;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

/** Converts persisted payment rows and metadata to/from the application payment model. */
@Component
final class PaymentRowMapper implements RowMapper<Payment> {

    private static final TypeReference<Map<String, String>> METADATA_TYPE = new TypeReference<>() {};

    private final ObjectMapper objectMapper;

    PaymentRowMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Payment mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new Payment(
                rs.getObject("id", java.util.UUID.class),
                rs.getObject("merchant_id", java.util.UUID.class),
                rs.getLong("amount"),
                rs.getString("currency"),
                PaymentStatus.valueOf(rs.getString("status")),
                rs.getLong("captured_amount"),
                rs.getLong("refunded_amount"),
                rs.getString("psp_ref"),
                deserializeMetadata(rs.getString("metadata_json")),
                rs.getLong("version"),
                rs.getLong("event_seq"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant());
    }

    String serializeMetadata(Map<String, String> metadata) {
        try {
            return objectMapper.writeValueAsString(metadata != null ? metadata : Collections.emptyMap());
        } catch (Exception ex) {
            throw new IllegalArgumentException("Payment metadata could not be serialized", ex);
        }
    }

    private Map<String, String> deserializeMetadata(String json) {
        if (json == null || json.isBlank()) return Collections.emptyMap();
        try {
            return objectMapper.readValue(json, METADATA_TYPE);
        } catch (Exception ex) {
            throw new IllegalStateException("Persisted payment metadata is invalid", ex);
        }
    }
}
