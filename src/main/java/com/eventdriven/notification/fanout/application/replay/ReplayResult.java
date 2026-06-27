package com.eventdriven.notification.fanout.application.replay;

import java.time.Instant;
import java.util.UUID;

/**
 * Summary of a subscription replay operation.
 */
public record ReplayResult(
        UUID subscriptionId,
        Instant from,
        Instant to,
        int eventsScanned,
        int matched,
        int queued,
        int skipped
) {
}
