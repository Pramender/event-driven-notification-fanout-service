package com.eventdriven.notification.fanout.domain;

import java.util.Map;

/**
 * HTTP webhook delivery target for a subscription.
 */
public record WebhookTarget(
        String url,
        Map<String, String> headers,
        int timeoutMs
) {
    public WebhookTarget {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("Webhook URL is required");
        }
        if (timeoutMs <= 0) {
            timeoutMs = 5000;
        }
        headers = headers == null ? Map.of() : Map.copyOf(headers);
    }
}
