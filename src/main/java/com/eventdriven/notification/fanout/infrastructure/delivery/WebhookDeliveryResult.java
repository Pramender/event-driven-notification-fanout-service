package com.eventdriven.notification.fanout.infrastructure.delivery;

/**
 * Result of a webhook HTTP call.
 */
public record WebhookDeliveryResult(int httpStatus) {
}
