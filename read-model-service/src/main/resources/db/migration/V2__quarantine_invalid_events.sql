CREATE TABLE read_model_poison_events (
    id              UUID PRIMARY KEY,
    payload_sha256  CHAR(64) NOT NULL UNIQUE,
    raw_payload     BYTEA NOT NULL,
    reason          TEXT NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
