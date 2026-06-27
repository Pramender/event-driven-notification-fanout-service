package com.eventdriven.notification.fanout.infrastructure.web.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

public record AcceptEventResponse(
        @JsonProperty("event_id") UUID eventId,
        String type,
        String source,
        JsonNode payload,
        @JsonProperty("occurred_at") Instant occurredAt,
        @JsonProperty("received_at") Instant receivedAt
) {
}
