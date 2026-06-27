package com.eventdriven.notification.fanout.domain;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

/**
 * Subscriber webhook configuration and filter rule.
 */
public record Subscription(
        UUID subscriptionId,
        String name,
        boolean enabled,
        DeliveryMode deliveryMode,
        JsonNode filter,
        WebhookTarget target,
        RetryPolicy retryPolicy,
        Instant createdAt,
        Instant updatedAt,
        boolean deleted
) {
    public boolean isActive() {
        return enabled && !deleted;
    }
}
