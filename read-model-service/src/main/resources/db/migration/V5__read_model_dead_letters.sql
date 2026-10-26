CREATE TABLE read_model_dead_letters (
    id                  UUID PRIMARY KEY,
    source_topic        VARCHAR(249) NOT NULL,
    source_partition    INTEGER NOT NULL,
    source_offset       BIGINT NOT NULL,
    message_key         VARCHAR(1024),
    raw_payload         BYTEA NOT NULL,
    exception_class     TEXT NOT NULL,
    exception_message   TEXT NOT NULL,
    failed_at           TIMESTAMPTZ NOT NULL,
    replay_count        INTEGER NOT NULL DEFAULT 0,
    last_replayed_at    TIMESTAMPTZ,
    UNIQUE (source_topic, source_partition, source_offset)
);

CREATE INDEX idx_read_model_dead_letters_failed_at
    ON read_model_dead_letters (failed_at DESC);
