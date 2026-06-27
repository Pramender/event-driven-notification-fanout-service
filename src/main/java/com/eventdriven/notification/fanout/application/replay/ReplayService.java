package com.eventdriven.notification.fanout.application.replay;

import com.eventdriven.notification.fanout.application.delivery.DeliveryEnqueueService;
import com.eventdriven.notification.fanout.application.exception.ReplayValidationException;
import com.eventdriven.notification.fanout.application.exception.ResourceNotFoundException;
import com.eventdriven.notification.fanout.application.filter.FilterEvaluator;
import com.eventdriven.notification.fanout.application.logging.LogActions;
import com.eventdriven.notification.fanout.application.logging.LogStatus;
import com.eventdriven.notification.fanout.application.logging.StructuredLog;
import com.eventdriven.notification.fanout.application.subscription.SubscriptionService;
import com.eventdriven.notification.fanout.domain.DeliveryMode;
import com.eventdriven.notification.fanout.domain.DeliveryStatus;
import com.eventdriven.notification.fanout.domain.InboundEvent;
import com.eventdriven.notification.fanout.domain.Subscription;
import com.eventdriven.notification.fanout.infrastructure.persistence.EntityMapper;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.EventEntity;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.SubscriptionDeliveryEntity;
import com.eventdriven.notification.fanout.infrastructure.persistence.repository.EventJpaRepository;
import com.eventdriven.notification.fanout.infrastructure.persistence.repository.SubscriptionDeliveryJpaRepository;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Re-enqueues historical events for a subscription within a time window.
 */
@Service
public class ReplayService {

    private static final Logger log = LoggerFactory.getLogger(ReplayService.class);

    private final SubscriptionService subscriptionService;
    private final EventJpaRepository eventRepository;
    private final SubscriptionDeliveryJpaRepository deliveryRepository;
    private final FilterEvaluator filterEvaluator;
    private final DeliveryEnqueueService deliveryEnqueueService;
    private final EntityMapper mapper;
    private final Tracer tracer;

    public ReplayService(
            SubscriptionService subscriptionService,
            EventJpaRepository eventRepository,
            SubscriptionDeliveryJpaRepository deliveryRepository,
            FilterEvaluator filterEvaluator,
            DeliveryEnqueueService deliveryEnqueueService,
            EntityMapper mapper,
            Tracer tracer) {
        this.subscriptionService = subscriptionService;
        this.eventRepository = eventRepository;
        this.deliveryRepository = deliveryRepository;
        this.filterEvaluator = filterEvaluator;
        this.deliveryEnqueueService = deliveryEnqueueService;
        this.mapper = mapper;
        this.tracer = tracer;
    }

    @Transactional
    public ReplayResult replay(UUID subscriptionId, Instant from, Instant to) {
        validateTimeRange(from, to);

        Span span = tracer.nextSpan().name("subscription.replay").start();
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            Subscription subscription = subscriptionService.get(subscriptionId);
            List<EventEntity> events = eventRepository.findByEffectiveTimestampBetween(from, to);

            int matched = 0;
            int queued = 0;
            int skipped = 0;

            for (EventEntity entity : events) {
                InboundEvent event = mapper.toDomain(entity);
                if (!filterEvaluator.matches(subscription.filter(), event)) {
                    continue;
                }
                matched++;

                Optional<SubscriptionDeliveryEntity> existing =
                        deliveryRepository.findByEventIdAndSubscriptionId(event.eventId(), subscriptionId);
                if (existing.isEmpty()) {
                    deliveryEnqueueService.enqueue(subscriptionId, event.eventId());
                    queued++;
                    continue;
                }

                SubscriptionDeliveryEntity delivery = existing.get();
                DeliveryStatus status = DeliveryStatus.valueOf(delivery.getStatus());
                if (status == DeliveryStatus.QUEUED
                        || status == DeliveryStatus.IN_FLIGHT
                        || status == DeliveryStatus.RETRY_PENDING) {
                    skipped++;
                    continue;
                }

                if (subscription.deliveryMode() == DeliveryMode.AT_MOST_ONCE
                        && status == DeliveryStatus.SENT) {
                    skipped++;
                    continue;
                }

                deliveryEnqueueService.requeue(delivery);
                queued++;
            }

            ReplayResult result = new ReplayResult(
                    subscriptionId, from, to, events.size(), matched, queued, skipped);

            StructuredLog.at(log)
                    .action(LogActions.REPLAY)
                    .status(LogStatus.SUCCESS)
                    .field("subscriptionId", subscriptionId)
                    .field("from", from)
                    .field("to", to)
                    .field("eventsScanned", events.size())
                    .field("matched", matched)
                    .field("queued", queued)
                    .field("skipped", skipped)
                    .message("Subscription replay completed")
                    .log();

            return result;
        } finally {
            span.end();
        }
    }

    private void validateTimeRange(Instant from, Instant to) {
        if (from == null || to == null) {
            throw new ReplayValidationException("Both 'from' and 'to' timestamps are required");
        }
        if (from.isAfter(to)) {
            throw new ReplayValidationException("'from' must be less than or equal to 'to'");
        }
    }
}
