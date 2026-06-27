package com.eventdriven.notification.fanout.infrastructure.web.dto;

import java.time.Instant;
import java.util.UUID;

public record ReplayEventsResponse(
        UUID subscriptionId,
        Instant from,
        Instant to,
        int eventsScanned,
        int matched,
        int queued,
        int skipped
) {
}
