CREATE TABLE IF NOT EXISTS audit_events (
    id BIGSERIAL PRIMARY KEY,
    event_id BIGINT NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,
    topic VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (event_type, event_id)
);

CREATE INDEX IF NOT EXISTS idx_audit_events_aggregate_id ON audit_events(aggregate_id);
CREATE INDEX IF NOT EXISTS idx_audit_events_event_type ON audit_events(event_type);