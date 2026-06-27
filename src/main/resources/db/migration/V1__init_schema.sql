-- Canonical inbound events (one row per accepted event).
CREATE TABLE events (
    event_id        UUID PRIMARY KEY,
    event_type      VARCHAR(255) NOT NULL,
    source          VARCHAR(255) NOT NULL,
    payload         JSONB NOT NULL,
    occurred_at     TIMESTAMPTZ,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    trace_id        VARCHAR(64)
);

CREATE INDEX idx_events_received_at ON events (received_at DESC);
CREATE INDEX idx_events_type_source ON events (event_type, source);

-- Subscriber definitions with JSON filter and webhook target.
CREATE TABLE subscriptions (
    subscription_id UUID PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    delivery_mode   VARCHAR(32) NOT NULL DEFAULT 'AT_LEAST_ONCE',
    filter_json     JSONB NOT NULL,
    target_json     JSONB NOT NULL,
    retry_policy_json JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted         BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_subscriptions_enabled ON subscriptions (enabled, deleted);

-- Per-subscription FIFO cursor (head-of-line pointer + sequence assigner).
CREATE TABLE subscription_delivery_cursors (
    subscription_id       UUID PRIMARY KEY REFERENCES subscriptions (subscription_id),
    next_deliverable_seq  BIGINT NOT NULL DEFAULT 1,
    next_assign_seq       BIGINT NOT NULL DEFAULT 1,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One delivery row per (event, subscription) match — primary tracking unit.
CREATE TABLE subscription_deliveries (
    delivery_id         UUID PRIMARY KEY,
    event_id            UUID NOT NULL REFERENCES events (event_id),
    subscription_id     UUID NOT NULL REFERENCES subscriptions (subscription_id),
    sequence_number     BIGINT NOT NULL,
    status              VARCHAR(32) NOT NULL,
    attempt_count       INT NOT NULL DEFAULT 0,
    next_retry_at       TIMESTAMPTZ,
    final_http_status   INT,
    error_reason        TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_delivery_event_subscription UNIQUE (event_id, subscription_id),
    CONSTRAINT uq_delivery_subscription_sequence UNIQUE (subscription_id, sequence_number)
);

CREATE INDEX idx_deliveries_subscription_status ON subscription_deliveries (subscription_id, status);
CREATE INDEX idx_deliveries_retry_pending ON subscription_deliveries (status, next_retry_at)
    WHERE status = 'RETRY_PENDING';
CREATE INDEX idx_deliveries_event ON subscription_deliveries (event_id);

-- Immutable audit log of each HTTP delivery attempt.
CREATE TABLE delivery_attempts (
    attempt_id      UUID PRIMARY KEY,
    delivery_id     UUID NOT NULL REFERENCES subscription_deliveries (delivery_id),
    attempt_number  INT NOT NULL,
    http_status     INT,
    error_reason    TEXT,
    status          VARCHAR(32) NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    finished_at     TIMESTAMPTZ NOT NULL,
    trace_id        VARCHAR(64),
    span_id         VARCHAR(32),
    CONSTRAINT uq_attempt_delivery_number UNIQUE (delivery_id, attempt_number)
);

CREATE INDEX idx_attempts_delivery ON delivery_attempts (delivery_id, attempt_number);
