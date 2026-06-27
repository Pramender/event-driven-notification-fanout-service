package com.eventdriven.notification.fanout.application.replay;

import com.eventdriven.notification.fanout.application.delivery.DeliveryEnqueueService;
import com.eventdriven.notification.fanout.application.exception.ReplayValidationException;
import com.eventdriven.notification.fanout.application.exception.ResourceNotFoundException;
import com.eventdriven.notification.fanout.application.filter.FilterEvaluator;
import com.eventdriven.notification.fanout.application.subscription.SubscriptionService;
import com.eventdriven.notification.fanout.domain.*;
import com.eventdriven.notification.fanout.infrastructure.persistence.EntityMapper;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.EventEntity;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.SubscriptionDeliveryEntity;
import com.eventdriven.notification.fanout.infrastructure.persistence.repository.EventJpaRepository;
import com.eventdriven.notification.fanout.infrastructure.persistence.repository.SubscriptionDeliveryJpaRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ReplayServiceTest {

    @Mock SubscriptionService subscriptionService;
    @Mock EventJpaRepository eventRepository;
    @Mock SubscriptionDeliveryJpaRepository deliveryRepository;
    @Mock FilterEvaluator filterEvaluator;
    @Mock DeliveryEnqueueService deliveryEnqueueService;
    @Mock Tracer tracer;
    @Mock Span span;
    @Mock TraceContext traceContext;

    ReplayService replayService;
    ObjectMapper objectMapper = new ObjectMapper();
    EntityMapper mapper = new EntityMapper(objectMapper);

    UUID subscriptionId = UUID.randomUUID();
    Instant from = Instant.parse("2026-06-27T10:00:00Z");
    Instant to = Instant.parse("2026-06-27T12:00:00Z");

    @BeforeEach
    void setUp() {
        replayService = new ReplayService(
                subscriptionService,
                eventRepository,
                deliveryRepository,
                filterEvaluator,
                deliveryEnqueueService,
                mapper,
                tracer);
        when(tracer.nextSpan()).thenReturn(span);
        when(span.name(any())).thenReturn(span);
        when(span.start()).thenReturn(span);
        when(tracer.withSpan(any())).thenReturn(() -> {});
    }

    @Test
    void replaysMatchingEventsWithoutExistingDeliveries() throws Exception {
        Subscription subscription = subscription(subscriptionId, DeliveryMode.AT_LEAST_ONCE);
        when(subscriptionService.get(subscriptionId)).thenReturn(subscription);

        EventEntity eventEntity = eventEntity(
                UUID.randomUUID(), "order.created", Instant.parse("2026-06-27T11:00:00Z"));
        when(eventRepository.findByEffectiveTimestampBetween(from, to)).thenReturn(List.of(eventEntity));
        when(filterEvaluator.matches(any(), any())).thenReturn(true);
        when(deliveryRepository.findByEventIdAndSubscriptionId(eventEntity.getEventId(), subscriptionId))
                .thenReturn(Optional.empty());

        ReplayResult result = replayService.replay(subscriptionId, from, to);

        assertThat(result.eventsScanned()).isEqualTo(1);
        assertThat(result.matched()).isEqualTo(1);
        assertThat(result.queued()).isEqualTo(1);
        assertThat(result.skipped()).isZero();
        verify(deliveryEnqueueService).enqueue(subscriptionId, eventEntity.getEventId());
    }

    @Test
    void skipsEventsThatDoNotMatchFilter() throws Exception {
        Subscription subscription = subscription(subscriptionId, DeliveryMode.AT_LEAST_ONCE);
        when(subscriptionService.get(subscriptionId)).thenReturn(subscription);

        EventEntity eventEntity = eventEntity(
                UUID.randomUUID(), "payment.completed", Instant.parse("2026-06-27T11:00:00Z"));
        when(eventRepository.findByEffectiveTimestampBetween(from, to)).thenReturn(List.of(eventEntity));
        when(filterEvaluator.matches(any(), any())).thenReturn(false);

        ReplayResult result = replayService.replay(subscriptionId, from, to);

        assertThat(result.matched()).isZero();
        assertThat(result.queued()).isZero();
        verifyNoInteractions(deliveryEnqueueService);
    }

    @Test
    void requeuesPreviouslySentEventsForAtLeastOnce() throws Exception {
        Subscription subscription = subscription(subscriptionId, DeliveryMode.AT_LEAST_ONCE);
        when(subscriptionService.get(subscriptionId)).thenReturn(subscription);

        EventEntity eventEntity = eventEntity(
                UUID.randomUUID(), "order.created", Instant.parse("2026-06-27T11:00:00Z"));
        when(eventRepository.findByEffectiveTimestampBetween(from, to)).thenReturn(List.of(eventEntity));
        when(filterEvaluator.matches(any(), any())).thenReturn(true);

        SubscriptionDeliveryEntity existing = new SubscriptionDeliveryEntity();
        existing.setDeliveryId(UUID.randomUUID());
        existing.setEventId(eventEntity.getEventId());
        existing.setSubscriptionId(subscriptionId);
        existing.setStatus(DeliveryStatus.SENT.name());
        when(deliveryRepository.findByEventIdAndSubscriptionId(eventEntity.getEventId(), subscriptionId))
                .thenReturn(Optional.of(existing));

        ReplayResult result = replayService.replay(subscriptionId, from, to);

        assertThat(result.queued()).isEqualTo(1);
        assertThat(result.skipped()).isZero();
        verify(deliveryEnqueueService).requeue(existing);
    }

    @Test
    void skipsAlreadySentEventsForAtMostOnce() throws Exception {
        Subscription subscription = subscription(subscriptionId, DeliveryMode.AT_MOST_ONCE);
        when(subscriptionService.get(subscriptionId)).thenReturn(subscription);

        EventEntity eventEntity = eventEntity(
                UUID.randomUUID(), "order.created", Instant.parse("2026-06-27T11:00:00Z"));
        when(eventRepository.findByEffectiveTimestampBetween(from, to)).thenReturn(List.of(eventEntity));
        when(filterEvaluator.matches(any(), any())).thenReturn(true);

        SubscriptionDeliveryEntity existing = new SubscriptionDeliveryEntity();
        existing.setStatus(DeliveryStatus.SENT.name());
        when(deliveryRepository.findByEventIdAndSubscriptionId(eventEntity.getEventId(), subscriptionId))
                .thenReturn(Optional.of(existing));

        ReplayResult result = replayService.replay(subscriptionId, from, to);

        assertThat(result.queued()).isZero();
        assertThat(result.skipped()).isEqualTo(1);
        verify(deliveryEnqueueService, never()).requeue(any());
    }

    @Test
    void skipsDeliveriesAlreadyInProgress() throws Exception {
        Subscription subscription = subscription(subscriptionId, DeliveryMode.AT_LEAST_ONCE);
        when(subscriptionService.get(subscriptionId)).thenReturn(subscription);

        EventEntity eventEntity = eventEntity(
                UUID.randomUUID(), "order.created", Instant.parse("2026-06-27T11:00:00Z"));
        when(eventRepository.findByEffectiveTimestampBetween(from, to)).thenReturn(List.of(eventEntity));
        when(filterEvaluator.matches(any(), any())).thenReturn(true);

        SubscriptionDeliveryEntity existing = new SubscriptionDeliveryEntity();
        existing.setStatus(DeliveryStatus.QUEUED.name());
        when(deliveryRepository.findByEventIdAndSubscriptionId(eventEntity.getEventId(), subscriptionId))
                .thenReturn(Optional.of(existing));

        ReplayResult result = replayService.replay(subscriptionId, from, to);

        assertThat(result.queued()).isZero();
        assertThat(result.skipped()).isEqualTo(1);
        verifyNoInteractions(deliveryEnqueueService);
    }

    @Test
    void requeuesPreviouslyFailedDeliveries() throws Exception {
        Subscription subscription = subscription(subscriptionId, DeliveryMode.AT_LEAST_ONCE);
        when(subscriptionService.get(subscriptionId)).thenReturn(subscription);

        EventEntity eventEntity = eventEntity(
                UUID.randomUUID(), "order.created", Instant.parse("2026-06-27T11:00:00Z"));
        when(eventRepository.findByEffectiveTimestampBetween(from, to)).thenReturn(List.of(eventEntity));
        when(filterEvaluator.matches(any(), any())).thenReturn(true);

        SubscriptionDeliveryEntity existing = new SubscriptionDeliveryEntity();
        existing.setStatus(DeliveryStatus.FAILED.name());
        when(deliveryRepository.findByEventIdAndSubscriptionId(eventEntity.getEventId(), subscriptionId))
                .thenReturn(Optional.of(existing));

        ReplayResult result = replayService.replay(subscriptionId, from, to);

        assertThat(result.queued()).isEqualTo(1);
        verify(deliveryEnqueueService).requeue(existing);
    }

    @Test
    void rejectsInvalidTimeRange() {
        assertThatThrownBy(() -> replayService.replay(subscriptionId, to, from))
                .isInstanceOf(ReplayValidationException.class)
                .hasMessageContaining("from");

        assertThatThrownBy(() -> replayService.replay(subscriptionId, null, to))
                .isInstanceOf(ReplayValidationException.class);
    }

    @Test
    void propagatesMissingSubscription() {
        when(subscriptionService.get(subscriptionId))
                .thenThrow(new ResourceNotFoundException("Subscription", subscriptionId));

        assertThatThrownBy(() -> replayService.replay(subscriptionId, from, to))
                .isInstanceOf(ResourceNotFoundException.class);
    }

    private Subscription subscription(UUID id, DeliveryMode mode) throws Exception {
        JsonNode filter = objectMapper.readTree("""
                { "all": [ { "field": "type", "op": "eq", "value": "order.created" } ] }
                """);
        return new Subscription(
                id,
                "replay-test",
                true,
                mode,
                filter,
                new WebhookTarget("http://localhost/hook", Map.of(), 3000),
                new RetryPolicy(3, 100, 500, 2.0),
                Instant.now(),
                Instant.now(),
                false
        );
    }

    private EventEntity eventEntity(UUID eventId, String type, Instant occurredAt) {
        EventEntity entity = new EventEntity();
        entity.setEventId(eventId);
        entity.setEventType(type);
        entity.setSource("test");
        entity.setPayload("{\"n\":1}");
        entity.setOccurredAt(occurredAt);
        entity.setReceivedAt(occurredAt);
        return entity;
    }
}
