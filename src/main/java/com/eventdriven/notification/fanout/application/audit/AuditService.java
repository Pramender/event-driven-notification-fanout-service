package com.eventdriven.notification.fanout.application.audit;

import com.eventdriven.notification.fanout.application.exception.ResourceNotFoundException;
import com.eventdriven.notification.fanout.domain.DeliveryAttemptRecord;
import com.eventdriven.notification.fanout.domain.DeliveryStatus;
import com.eventdriven.notification.fanout.domain.SubscriptionDelivery;
import com.eventdriven.notification.fanout.infrastructure.persistence.EntityMapper;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.SubscriptionDeliveryEntity;
import com.eventdriven.notification.fanout.infrastructure.persistence.repository.DeliveryAttemptJpaRepository;
import com.eventdriven.notification.fanout.infrastructure.persistence.repository.SubscriptionDeliveryJpaRepository;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Read-only audit queries for delivery history per event or subscription.
 */
@Service
public class AuditService {

    private final SubscriptionDeliveryJpaRepository deliveryRepository;
    private final DeliveryAttemptJpaRepository attemptRepository;
    private final EntityMapper mapper;
    private final Tracer tracer;

    public AuditService(
            SubscriptionDeliveryJpaRepository deliveryRepository,
            DeliveryAttemptJpaRepository attemptRepository,
            EntityMapper mapper,
            Tracer tracer) {
        this.deliveryRepository = deliveryRepository;
        this.attemptRepository = attemptRepository;
        this.mapper = mapper;
        this.tracer = tracer;
    }

    @Transactional(readOnly = true)
    public List<DeliveryAuditView> byEvent(UUID eventId) {
        Span span = tracer.nextSpan().name("audit.query.by_event").start();
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            return deliveryRepository.findByEventId(eventId).stream()
                    .map(this::toAuditView)
                    .toList();
        } finally {
            span.end();
        }
    }

    @Transactional(readOnly = true)
    public List<DeliveryAuditView> bySubscription(UUID subscriptionId, DeliveryStatus statusFilter) {
        Span span = tracer.nextSpan().name("audit.query.by_subscription").start();
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            List<SubscriptionDeliveryEntity> deliveries = statusFilter == null
                    ? deliveryRepository.findBySubscriptionId(subscriptionId)
                    : deliveryRepository.findBySubscriptionIdAndStatus(subscriptionId, statusFilter.name());
            return deliveries.stream().map(this::toAuditView).toList();
        } finally {
            span.end();
        }
    }

    @Transactional(readOnly = true)
    public DeliveryAuditView byDeliveryId(UUID deliveryId) {
        SubscriptionDeliveryEntity delivery = deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new ResourceNotFoundException("Delivery", deliveryId));
        return toAuditView(delivery);
    }

    private DeliveryAuditView toAuditView(SubscriptionDeliveryEntity entity) {
        SubscriptionDelivery delivery = mapper.toDomain(entity);
        List<DeliveryAttemptRecord> attempts = attemptRepository
                .findByDeliveryIdOrderByAttemptNumberAsc(entity.getDeliveryId())
                .stream()
                .map(mapper::toDomain)
                .toList();
        return new DeliveryAuditView(
                delivery.eventId(),
                delivery.subscriptionId(),
                delivery.deliveryId(),
                delivery.sequenceNumber(),
                delivery.status(),
                attempts
        );
    }
}
