package com.eventdriven.notification.fanout.infrastructure.web.dto;

import com.eventdriven.notification.fanout.domain.DeliveryMode;
import com.eventdriven.notification.fanout.domain.RetryPolicy;
import com.eventdriven.notification.fanout.domain.WebhookTarget;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

public record SubscriptionResponse(
        UUID subscriptionId,
        String name,
        boolean enabled,
        DeliveryMode deliveryMode,
        JsonNode filter,
        WebhookTarget target,
        RetryPolicy retryPolicy,
        Instant createdAt,
        Instant updatedAt
) {
}
