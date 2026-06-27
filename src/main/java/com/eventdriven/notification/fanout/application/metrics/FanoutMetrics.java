package com.eventdriven.notification.fanout.application.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * Central Micrometer counters for fanout pipeline observability.
 */
@Component
public class FanoutMetrics {

    private final Counter eventsAccepted;
    private final Counter eventsInvalid;
    private final Counter fanoutMatches;
    private final Counter deliveriesAttempted;
    private final Counter deliveriesSuccess;
    private final Counter deliveriesFailed;

    public FanoutMetrics(MeterRegistry registry) {
        this.eventsAccepted = Counter.builder("events.accepted.total")
                .description("Total accepted inbound events")
                .register(registry);
        this.eventsInvalid = Counter.builder("events.invalid.total")
                .description("Total rejected inbound events")
                .register(registry);
        this.fanoutMatches = Counter.builder("fanout.matches.total")
                .description("Total subscription matches during fanout")
                .register(registry);
        this.deliveriesAttempted = Counter.builder("deliveries.attempts.total")
                .description("Total delivery HTTP attempts")
                .register(registry);
        this.deliveriesSuccess = Counter.builder("deliveries.success.total")
                .description("Total successful deliveries")
                .register(registry);
        this.deliveriesFailed = Counter.builder("deliveries.failed.total")
                .description("Total permanently failed deliveries")
                .register(registry);
    }

    public void eventAccepted(String type, String source) {
        eventsAccepted.increment();
    }

    public void eventInvalid(String reason) {
        eventsInvalid.increment();
    }

    public void fanoutMatch(String subscriptionId) {
        fanoutMatches.increment();
    }

    public void deliveryAttempted(String subscriptionId) {
        deliveriesAttempted.increment();
    }

    public void deliverySuccess(String subscriptionId) {
        deliveriesSuccess.increment();
    }

    public void deliveryFailed(String subscriptionId) {
        deliveriesFailed.increment();
    }
}
