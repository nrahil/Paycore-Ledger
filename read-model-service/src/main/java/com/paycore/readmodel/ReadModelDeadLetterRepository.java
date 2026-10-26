package com.paycore.readmodel;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Repository;

/** Persists Kafka dead letters before their source offsets can be considered handled. */
@Repository
public class ReadModelDeadLetterRepository {

    private final JdbcClient jdbcClient;

    public ReadModelDeadLetterRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    public void save(ConsumerRecord<String, byte[]> record, Instant failedAt) {
        String sourceTopic = requiredUtf8(record, KafkaHeaders.DLT_ORIGINAL_TOPIC);
        if (!ReadModelKafkaEventListener.PAYMENTS_EVENTS_TOPIC.equals(sourceTopic)) {
            throw new IllegalArgumentException("Unexpected dead-letter source topic: " + sourceTopic);
        }
        int sourcePartition = requiredInt(record, KafkaHeaders.DLT_ORIGINAL_PARTITION);
        long sourceOffset = requiredLong(record, KafkaHeaders.DLT_ORIGINAL_OFFSET);
        String exceptionClass = optionalUtf8(record, KafkaHeaders.DLT_EXCEPTION_FQCN).orElse("UnknownException");
        String exceptionMessage = optionalUtf8(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE).orElse("No failure message supplied");
        byte[] payload = record.value();
        if (payload == null) throw new IllegalArgumentException("Dead-letter payload is empty");

        jdbcClient.sql("""
                INSERT INTO read_model_dead_letters (
                    id, source_topic, source_partition, source_offset, message_key, raw_payload,
                    exception_class, exception_message, failed_at
                ) VALUES (
                    :id, :topic, :partition, :offset, :messageKey, :payload,
                    :exceptionClass, :exceptionMessage, :failedAt
                )
                ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING
                """)
                .param("id", UUID.randomUUID())
                .param("topic", sourceTopic)
                .param("partition", sourcePartition)
                .param("offset", sourceOffset)
                .param("messageKey", record.key())
                .param("payload", payload)
                .param("exceptionClass", exceptionClass)
                .param("exceptionMessage", exceptionMessage)
                .param("failedAt", Timestamp.from(failedAt))
                .update();
    }

    public List<ReadModelViews.DeadLetterView> list(int limit) {
        return jdbcClient.sql("""
                SELECT id, source_topic, source_partition, source_offset, message_key,
                       exception_class, exception_message, failed_at, replay_count, last_replayed_at
                FROM read_model_dead_letters
                ORDER BY failed_at DESC, id
                LIMIT :limit
                """)
                .param("limit", limit)
                .query((rs, rowNum) -> new ReadModelViews.DeadLetterView(
                        rs.getObject("id", UUID.class),
                        rs.getString("source_topic"),
                        rs.getInt("source_partition"),
                        rs.getLong("source_offset"),
                        rs.getString("message_key"),
                        rs.getString("exception_class"),
                        rs.getString("exception_message"),
                        rs.getTimestamp("failed_at").toInstant(),
                        rs.getInt("replay_count"),
                        Optional.ofNullable(rs.getTimestamp("last_replayed_at")).map(Timestamp::toInstant).orElse(null)))
                .list();
    }

    public Optional<ReadModelDeadLetter> find(UUID id) {
        return jdbcClient.sql("""
                SELECT id, source_topic, message_key, raw_payload
                FROM read_model_dead_letters WHERE id = :id
                """)
                .param("id", id)
                .query((rs, rowNum) -> new ReadModelDeadLetter(
                        rs.getObject("id", UUID.class),
                        rs.getString("source_topic"),
                        rs.getString("message_key"),
                        rs.getBytes("raw_payload")))
                .optional();
    }

    public void markReplayed(UUID id, Instant at) {
        int updated = jdbcClient.sql("""
                UPDATE read_model_dead_letters
                SET replay_count = replay_count + 1, last_replayed_at = :at
                WHERE id = :id
                """)
                .param("at", Timestamp.from(at))
                .param("id", id)
                .update();
        if (updated != 1) throw new IllegalStateException("Dead-letter record disappeared during replay: " + id);
    }

    public ReadModelViews.DeadLetterReplayResult findReplayResult(UUID id, Instant replayedAt) {
        return jdbcClient.sql("""
                SELECT id, source_topic, replay_count FROM read_model_dead_letters WHERE id = :id
                """)
                .param("id", id)
                .query((rs, rowNum) -> new ReadModelViews.DeadLetterReplayResult(
                        rs.getObject("id", UUID.class),
                        rs.getString("source_topic"),
                        rs.getInt("replay_count"),
                        replayedAt))
                .optional()
                .orElseThrow(() -> new IllegalStateException("Replayed dead-letter event not found: " + id));
    }

    private static String requiredUtf8(ConsumerRecord<String, byte[]> record, String headerName) {
        return optionalUtf8(record, headerName)
                .orElseThrow(() -> new IllegalArgumentException("Missing Kafka header " + headerName));
    }

    private static Optional<String> optionalUtf8(ConsumerRecord<String, byte[]> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        return header == null ? Optional.empty() : Optional.of(new String(header.value(), StandardCharsets.UTF_8));
    }

    private static int requiredInt(ConsumerRecord<String, byte[]> record, String headerName) {
        byte[] value = requiredHeader(record, headerName);
        if (value.length != Integer.BYTES) throw new IllegalArgumentException("Invalid Kafka header " + headerName);
        return ByteBuffer.wrap(value).getInt();
    }

    private static long requiredLong(ConsumerRecord<String, byte[]> record, String headerName) {
        byte[] value = requiredHeader(record, headerName);
        if (value.length != Long.BYTES) throw new IllegalArgumentException("Invalid Kafka header " + headerName);
        return ByteBuffer.wrap(value).getLong();
    }

    private static byte[] requiredHeader(ConsumerRecord<String, byte[]> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        if (header == null) throw new IllegalArgumentException("Missing Kafka header " + headerName);
        return header.value();
    }

    public record ReadModelDeadLetter(UUID id, String sourceTopic, String messageKey, byte[] rawPayload) {}
}
