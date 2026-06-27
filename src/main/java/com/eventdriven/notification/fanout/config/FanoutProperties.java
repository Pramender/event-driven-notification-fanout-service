package com.eventdriven.notification.fanout.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "fanout")
public record FanoutProperties(
        Kafka kafka,
        Delivery delivery,
        long subscriptionCacheTtlSeconds
) {
    public record Kafka(String inboundTopic, String deliveryTopic, String dlqTopic) {
    }

    public record Delivery(long schedulerIntervalMs, long workerPollIntervalMs) {
    }
}
