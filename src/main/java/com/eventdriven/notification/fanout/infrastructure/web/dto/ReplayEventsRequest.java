package com.eventdriven.notification.fanout.infrastructure.web.dto;

import jakarta.validation.constraints.NotNull;

import java.time.Instant;

public record ReplayEventsRequest(
        @NotNull Instant from,
        @NotNull Instant to
) {
}
