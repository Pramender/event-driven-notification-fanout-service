package com.eventdriven.notification.fanout.application.fanout;

import com.eventdriven.notification.fanout.application.delivery.DeliveryEnqueueService;
import com.eventdriven.notification.fanout.application.filter.FilterEvaluator;
import com.eventdriven.notification.fanout.application.logging.LogActions;
import com.eventdriven.notification.fanout.application.logging.LogStatus;
import com.eventdriven.notification.fanout.application.logging.StructuredLog;
import com.eventdriven.notification.fanout.application.metrics.FanoutMetrics;
import com.eventdriven.notification.fanout.application.subscription.SubscriptionCache;
import com.eventdriven.notification.fanout.domain.InboundEvent;
import com.eventdriven.notification.fanout.domain.Subscription;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Evaluates active subscriptions against an event and enqueues per-subscriber deliveries with FIFO sequence numbers.
 */
@Service
public class FanoutService {

    private static final Logger log = LoggerFactory.getLogger(FanoutService.class);

    private final SubscriptionCache subscriptionCache;
    private final FilterEvaluator filterEvaluator;
    private final DeliveryEnqueueService deliveryEnqueueService;
    private final FanoutMetrics metrics;
    private final Tracer tracer;

    public FanoutService(
            SubscriptionCache subscriptionCache,
            FilterEvaluator filterEvaluator,
            DeliveryEnqueueService deliveryEnqueueService,
            FanoutMetrics metrics,
            Tracer tracer) {
        this.subscriptionCache = subscriptionCache;
        this.filterEvaluator = filterEvaluator;
        this.deliveryEnqueueService = deliveryEnqueueService;
        this.metrics = metrics;
        this.tracer = tracer;
    }

    @Transactional
    public int fanout(InboundEvent event) {
        Span span = tracer.nextSpan().name("fanout.evaluate").start();
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            List<Subscription> subscriptions = subscriptionCache.getActiveSubscriptions();
            int matchCount = 0;
            for (Subscription subscription : subscriptions) {
                Span matchSpan = tracer.nextSpan().name("subscription.match").start();
                try (Tracer.SpanInScope matchScope = tracer.withSpan(matchSpan)) {
                    if (!filterEvaluator.matches(subscription.filter(), event)) {
                        continue;
                    }
                    createQueuedDelivery(event, subscription);
                    metrics.fanoutMatch(subscription.subscriptionId().toString());
                    matchCount++;
                    StructuredLog.at(log)
                            .action(LogActions.FANOUT_MATCH)
                            .status(LogStatus.SUCCESS)
                            .field("eventId", event.eventId())
                            .field("subscriptionId", subscription.subscriptionId())
                            .message("Event matched subscription")
                            .log();
                } finally {
                    matchSpan.end();
                }
            }
            StructuredLog.at(log)
                    .action(LogActions.FANOUT_COMPLETE)
                    .status(LogStatus.SUCCESS)
                    .field("eventId", event.eventId())
                    .field("matches", matchCount)
                    .message("Fanout complete")
                    .log();
            return matchCount;
        } finally {
            span.end();
        }
    }

    private void createQueuedDelivery(InboundEvent event, Subscription subscription) {
        deliveryEnqueueService.enqueue(subscription.subscriptionId(), event.eventId());
    }
}
