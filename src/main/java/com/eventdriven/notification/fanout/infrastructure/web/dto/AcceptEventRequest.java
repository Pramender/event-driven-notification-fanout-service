package com.eventdriven.notification.fanout.infrastructure.web.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record AcceptEventRequest(
        @JsonProperty("event_id") UUID eventId,
        @NotBlank String type,
        @NotBlank String source,
        @JsonProperty("occurred_at") Instant occurredAt,
        @NotNull JsonNode payload
) {
}
