package com.eventdriven.notification.fanout.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * Tracks delivery state for one (event, subscription) pair with FIFO sequence per subscription.
 */
public record SubscriptionDelivery(
        UUID deliveryId,
        UUID eventId,
        UUID subscriptionId,
        long sequenceNumber,
        DeliveryStatus status,
        int attemptCount,
        Instant nextRetryAt,
        Integer finalHttpStatus,
        String errorReason,
        Instant createdAt,
        Instant updatedAt
) {
}
