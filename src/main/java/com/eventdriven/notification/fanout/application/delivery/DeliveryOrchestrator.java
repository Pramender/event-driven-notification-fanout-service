package com.eventdriven.notification.fanout.application.delivery;

import com.eventdriven.notification.fanout.application.exception.PermanentDeliveryException;
import com.eventdriven.notification.fanout.application.exception.RetryableDeliveryException;
import com.eventdriven.notification.fanout.application.logging.MdcContext;
import com.eventdriven.notification.fanout.application.metrics.FanoutMetrics;
import com.eventdriven.notification.fanout.application.subscription.SubscriptionCache;
import com.eventdriven.notification.fanout.domain.*;
import com.eventdriven.notification.fanout.infrastructure.delivery.WebhookDeliveryClient;
import com.eventdriven.notification.fanout.infrastructure.delivery.WebhookDeliveryResult;
import com.eventdriven.notification.fanout.infrastructure.persistence.EntityMapper;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.DeliveryAttemptEntity;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.EventEntity;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.SubscriptionDeliveryCursorEntity;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.SubscriptionDeliveryEntity;
import com.eventdriven.notification.fanout.infrastructure.persistence.repository.*;
import io.micrometer.core.instrument.Timer;
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
 * Dispatches webhook deliveries in strict FIFO order per subscription (head-of-line blocking).
 */
@Service
public class DeliveryOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(DeliveryOrchestrator.class);
    private static final int BATCH_SIZE = 20;

    private final SubscriptionDeliveryJpaRepository deliveryRepository;
    private final SubscriptionDeliveryCursorJpaRepository cursorRepository;
    private final DeliveryAttemptJpaRepository attemptRepository;
    private final EventJpaRepository eventRepository;
    private final SubscriptionCache subscriptionCache;
    private final WebhookDeliveryClient webhookClient;
    private final EntityMapper mapper;
    private final FanoutMetrics metrics;
    private final Timer deliveryLatencyTimer;
    private final Tracer tracer;

    public DeliveryOrchestrator(
            SubscriptionDeliveryJpaRepository deliveryRepository,
            SubscriptionDeliveryCursorJpaRepository cursorRepository,
            DeliveryAttemptJpaRepository attemptRepository,
            EventJpaRepository eventRepository,
            SubscriptionCache subscriptionCache,
            WebhookDeliveryClient webhookClient,
            EntityMapper mapper,
            FanoutMetrics metrics,
            Timer deliveryLatencyTimer,
            Tracer tracer) {
        this.deliveryRepository = deliveryRepository;
        this.cursorRepository = cursorRepository;
        this.attemptRepository = attemptRepository;
        this.eventRepository = eventRepository;
        this.subscriptionCache = subscriptionCache;
        this.webhookClient = webhookClient;
        this.mapper = mapper;
        this.metrics = metrics;
        this.deliveryLatencyTimer = deliveryLatencyTimer;
        this.tracer = tracer;
    }

    /**
     * Polls deliveries eligible at the head-of-line and attempts HTTP dispatch.
     */
    public void processReadyDeliveries() {
        List<SubscriptionDeliveryEntity> ready =
                deliveryRepository.findReadyForDispatch(Instant.now(), BATCH_SIZE);
        for (SubscriptionDeliveryEntity delivery : ready) {
            try {
                dispatchDelivery(delivery.getDeliveryId());
            } catch (Exception ex) {
                log.error("Unexpected error dispatching deliveryId={}", delivery.getDeliveryId(), ex);
            }
        }
    }

    @Transactional
    public void dispatchDelivery(UUID deliveryId) {
        SubscriptionDeliveryEntity delivery = deliveryRepository.findForUpdate(deliveryId)
                .orElseThrow(() -> new IllegalArgumentException("Delivery not found: " + deliveryId));

        MdcContext.putDelivery(delivery.getDeliveryId(), delivery.getSubscriptionId(), delivery.getSequenceNumber());

        if (delivery.getStatus().equals(DeliveryStatus.SENT.name())
                || delivery.getStatus().equals(DeliveryStatus.FAILED.name())) {
            return;
        }

        Subscription subscription = subscriptionCache.getById(delivery.getSubscriptionId())
                .orElseThrow(() -> new IllegalStateException("Subscription missing: " + delivery.getSubscriptionId()));

        if (subscription.deliveryMode() == DeliveryMode.AT_MOST_ONCE
                && deliveryRepository.existsByEventIdAndSubscriptionIdAndStatus(
                delivery.getEventId(), delivery.getSubscriptionId(), DeliveryStatus.SENT.name())) {
            markTerminal(delivery, DeliveryStatus.FAILED, null, "Already delivered (AT_MOST_ONCE)");
            return;
        }

        DeliveryStatus current = DeliveryStatus.valueOf(delivery.getStatus());
        if (current == DeliveryStatus.QUEUED || current == DeliveryStatus.RETRY_PENDING) {
            DeliveryStateMachine.assertTransition(current, DeliveryStatus.IN_FLIGHT);
            delivery.setStatus(DeliveryStatus.IN_FLIGHT.name());
            delivery.setUpdatedAt(Instant.now());
        }

        EventEntity eventEntity = eventRepository.findById(delivery.getEventId())
                .orElseThrow(() -> new IllegalStateException("Event missing: " + delivery.getEventId()));
        InboundEvent event = mapper.toDomain(eventEntity);

        int attemptNumber = delivery.getAttemptCount() + 1;
        delivery.setAttemptCount(attemptNumber);

        Span span = tracer.nextSpan().name("delivery.http").start();
        Instant startedAt = Instant.now();
        DeliveryAttemptEntity attempt = new DeliveryAttemptEntity();
        attempt.setAttemptId(UUID.randomUUID());
        attempt.setDeliveryId(delivery.getDeliveryId());
        attempt.setAttemptNumber(attemptNumber);
        attempt.setStartedAt(startedAt);
        attempt.setTraceId(span.context().traceId());
        attempt.setSpanId(span.context().spanId());

        Timer.Sample sample = Timer.start();
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            metrics.deliveryAttempted(delivery.getSubscriptionId().toString());
            WebhookDeliveryResult result = webhookClient.deliver(
                    subscription,
                    event,
                    delivery.getDeliveryId(),
                    attemptNumber
            );
            sample.stop(deliveryLatencyTimer);
            attempt.setFinishedAt(Instant.now());
            attempt.setHttpStatus(result.httpStatus());
            attempt.setStatus(DeliveryStatus.SENT.name());
            attemptRepository.save(attempt);

            delivery.setStatus(DeliveryStatus.SENT.name());
            delivery.setFinalHttpStatus(result.httpStatus());
            delivery.setErrorReason(null);
            delivery.setNextRetryAt(null);
            delivery.setUpdatedAt(Instant.now());
            deliveryRepository.save(delivery);

            advanceHeadOfLine(delivery.getSubscriptionId(), delivery.getSequenceNumber());
            metrics.deliverySuccess(delivery.getSubscriptionId().toString());
            log.info("Delivery succeeded deliveryId={} attempt={} httpStatus={}",
                    deliveryId, attemptNumber, result.httpStatus());
        } catch (RetryableDeliveryException ex) {
            handleFailure(delivery, attempt, startedAt, ex.getHttpStatus(), ex.getMessage(), true, subscription.retryPolicy());
        } catch (PermanentDeliveryException ex) {
            handleFailure(delivery, attempt, startedAt, ex.getHttpStatus(), ex.getMessage(), false, subscription.retryPolicy());
        } catch (Exception ex) {
            handleFailure(delivery, attempt, startedAt, null, ex.getMessage(), true, subscription.retryPolicy());
        } finally {
            span.end();
            MdcContext.clear();
        }
    }

    private void handleFailure(
            SubscriptionDeliveryEntity delivery,
            DeliveryAttemptEntity attempt,
            Instant startedAt,
            Integer httpStatus,
            String reason,
            boolean retryable,
            RetryPolicy retryPolicy) {
        attempt.setFinishedAt(Instant.now());
        attempt.setHttpStatus(httpStatus);
        attempt.setErrorReason(truncate(reason));
        delivery.setFinalHttpStatus(httpStatus);
        delivery.setErrorReason(truncate(reason));

        int attemptNumber = delivery.getAttemptCount();
        boolean exhausted = attemptNumber >= retryPolicy.maxAttempts();

        if (retryable && !exhausted) {
            DeliveryStateMachine.assertTransition(DeliveryStatus.IN_FLIGHT, DeliveryStatus.RETRY_PENDING);
            attempt.setStatus(DeliveryStatus.RETRY_PENDING.name());
            attemptRepository.save(attempt);

            long backoffMs = retryPolicy.computeBackoffMs(attemptNumber);
            delivery.setStatus(DeliveryStatus.RETRY_PENDING.name());
            delivery.setNextRetryAt(Instant.now().plusMillis(backoffMs));
            delivery.setUpdatedAt(Instant.now());
            deliveryRepository.save(delivery);
            log.warn("Delivery retry scheduled deliveryId={} attempt={} nextRetryInMs={} reason={}",
                    delivery.getDeliveryId(), attemptNumber, backoffMs, reason);
        } else {
            DeliveryStateMachine.assertTransition(DeliveryStatus.IN_FLIGHT, DeliveryStatus.FAILED);
            attempt.setStatus(DeliveryStatus.FAILED.name());
            attemptRepository.save(attempt);

            markTerminal(delivery, DeliveryStatus.FAILED, httpStatus, reason);
            advanceHeadOfLine(delivery.getSubscriptionId(), delivery.getSequenceNumber());
            metrics.deliveryFailed(delivery.getSubscriptionId().toString());
            log.error("Delivery permanently failed deliveryId={} attempt={} httpStatus={} reason={}",
                    delivery.getDeliveryId(), attemptNumber, httpStatus, reason);
        }
    }

    private void markTerminal(
            SubscriptionDeliveryEntity delivery,
            DeliveryStatus status,
            Integer httpStatus,
            String reason) {
        delivery.setStatus(status.name());
        delivery.setFinalHttpStatus(httpStatus);
        delivery.setErrorReason(truncate(reason));
        delivery.setNextRetryAt(null);
        delivery.setUpdatedAt(Instant.now());
        deliveryRepository.save(delivery);
    }

    /**
     * Advances the subscription head-of-line cursor when the current sequence reaches a terminal state.
     */
    private void advanceHeadOfLine(UUID subscriptionId, long completedSequence) {
        SubscriptionDeliveryCursorEntity cursor = cursorRepository.findForUpdate(subscriptionId)
                .orElseThrow(() -> new IllegalStateException("Missing cursor for subscription " + subscriptionId));
        if (cursor.getNextDeliverableSeq() == completedSequence) {
            cursor.setNextDeliverableSeq(completedSequence + 1);
            cursor.setUpdatedAt(Instant.now());
            cursorRepository.save(cursor);
            log.debug("Advanced head-of-line subscriptionId={} nextDeliverableSeq={}",
                    subscriptionId, cursor.getNextDeliverableSeq());
        }
    }

    private String truncate(String value) {
        if (value == null) {
            return null;
        }
        return value.length() <= 500 ? value : value.substring(0, 500);
    }
}
