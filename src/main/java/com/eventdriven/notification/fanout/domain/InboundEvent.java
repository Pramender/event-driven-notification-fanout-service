package com.eventdriven.notification.fanout.domain;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

/**
 * Canonical accepted inbound event.
 */
public record InboundEvent(
        UUID eventId,
        String type,
        String source,
        JsonNode payload,
        Instant occurredAt,
        Instant receivedAt,
        String traceId
) {
}
