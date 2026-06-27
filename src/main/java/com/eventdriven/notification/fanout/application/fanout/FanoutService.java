package com.eventdriven.notification.fanout.application.fanout;

import com.eventdriven.notification.fanout.application.filter.FilterEvaluator;
import com.eventdriven.notification.fanout.application.metrics.FanoutMetrics;
import com.eventdriven.notification.fanout.application.subscription.SubscriptionCache;
import com.eventdriven.notification.fanout.domain.DeliveryStatus;
import com.eventdriven.notification.fanout.domain.InboundEvent;
import com.eventdriven.notification.fanout.domain.Subscription;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.SubscriptionDeliveryCursorEntity;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.SubscriptionDeliveryEntity;
import com.eventdriven.notification.fanout.infrastructure.persistence.repository.SubscriptionDeliveryCursorJpaRepository;
import com.eventdriven.notification.fanout.infrastructure.persistence.repository.SubscriptionDeliveryJpaRepository;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Evaluates active subscriptions against an event and enqueues per-subscriber deliveries with FIFO sequence numbers.
 */
@Service
public class FanoutService {

    private static final Logger log = LoggerFactory.getLogger(FanoutService.class);

    private final SubscriptionCache subscriptionCache;
    private final FilterEvaluator filterEvaluator;
    private final SubscriptionDeliveryJpaRepository deliveryRepository;
    private final SubscriptionDeliveryCursorJpaRepository cursorRepository;
    private final FanoutMetrics metrics;
    private final Tracer tracer;

    public FanoutService(
            SubscriptionCache subscriptionCache,
            FilterEvaluator filterEvaluator,
            SubscriptionDeliveryJpaRepository deliveryRepository,
            SubscriptionDeliveryCursorJpaRepository cursorRepository,
            FanoutMetrics metrics,
            Tracer tracer) {
        this.subscriptionCache = subscriptionCache;
        this.filterEvaluator = filterEvaluator;
        this.deliveryRepository = deliveryRepository;
        this.cursorRepository = cursorRepository;
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
                    log.info("Event matched subscription eventId={} subscriptionId={}",
                            event.eventId(), subscription.subscriptionId());
                } finally {
                    matchSpan.end();
                }
            }
            log.info("Fanout complete eventId={} matches={}", event.eventId(), matchCount);
            return matchCount;
        } finally {
            span.end();
        }
    }

    private void createQueuedDelivery(InboundEvent event, Subscription subscription) {
        UUID subscriptionId = subscription.subscriptionId();
        SubscriptionDeliveryCursorEntity cursor = cursorRepository.findForUpdate(subscriptionId)
                .orElseGet(() -> initializeCursor(subscriptionId));

        long sequenceNumber = cursor.getNextAssignSeq();
        cursor.setNextAssignSeq(sequenceNumber + 1);
        cursor.setUpdatedAt(Instant.now());
        cursorRepository.save(cursor);

        SubscriptionDeliveryEntity delivery = new SubscriptionDeliveryEntity();
        delivery.setDeliveryId(UUID.randomUUID());
        delivery.setEventId(event.eventId());
        delivery.setSubscriptionId(subscriptionId);
        delivery.setSequenceNumber(sequenceNumber);
        delivery.setStatus(DeliveryStatus.QUEUED.name());
        delivery.setAttemptCount(0);
        delivery.setCreatedAt(Instant.now());
        delivery.setUpdatedAt(Instant.now());
        deliveryRepository.save(delivery);
    }

    private SubscriptionDeliveryCursorEntity initializeCursor(UUID subscriptionId) {
        SubscriptionDeliveryCursorEntity cursor = new SubscriptionDeliveryCursorEntity();
        cursor.setSubscriptionId(subscriptionId);
        cursor.setNextDeliverableSeq(1);
        cursor.setNextAssignSeq(1);
        cursor.setUpdatedAt(Instant.now());
        return cursorRepository.save(cursor);
    }
}
