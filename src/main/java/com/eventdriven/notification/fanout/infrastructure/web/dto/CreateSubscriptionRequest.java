package com.eventdriven.notification.fanout.infrastructure.web.dto;

import com.eventdriven.notification.fanout.domain.DeliveryMode;
import com.eventdriven.notification.fanout.domain.RetryPolicy;
import com.eventdriven.notification.fanout.domain.WebhookTarget;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record CreateSubscriptionRequest(
        @NotBlank String name,
        @NotNull DeliveryMode deliveryMode,
        @NotNull JsonNode filter,
        @NotNull @Valid WebhookTarget target,
        @NotNull @Valid RetryPolicy retryPolicy
) {
}
