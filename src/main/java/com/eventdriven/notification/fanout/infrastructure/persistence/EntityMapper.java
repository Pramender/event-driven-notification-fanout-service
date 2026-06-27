package com.eventdriven.notification.fanout.infrastructure.persistence;

import com.eventdriven.notification.fanout.domain.*;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

/**
 * Maps between JPA entities and domain records.
 */
@Component
public class EntityMapper {

    private final ObjectMapper objectMapper;

    public EntityMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public InboundEvent toDomain(EventEntity entity) {
        try {
            JsonNode payload = objectMapper.readTree(entity.getPayload());
            return new InboundEvent(
                    entity.getEventId(),
                    entity.getEventType(),
                    entity.getSource(),
                    payload,
                    entity.getOccurredAt(),
                    entity.getReceivedAt(),
                    entity.getTraceId()
            );
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Invalid event payload JSON for event " + entity.getEventId(), ex);
        }
    }

    public Subscription toDomain(SubscriptionEntity entity) {
        try {
            return new Subscription(
                    entity.getSubscriptionId(),
                    entity.getName(),
                    entity.isEnabled(),
                    DeliveryMode.valueOf(entity.getDeliveryMode()),
                    objectMapper.readTree(entity.getFilterJson()),
                    objectMapper.readValue(entity.getTargetJson(), WebhookTarget.class),
                    objectMapper.readValue(entity.getRetryPolicyJson(), RetryPolicy.class),
                    entity.getCreatedAt(),
                    entity.getUpdatedAt(),
                    entity.isDeleted()
            );
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Invalid subscription JSON for " + entity.getSubscriptionId(), ex);
        }
    }

    public SubscriptionDelivery toDomain(SubscriptionDeliveryEntity entity) {
        return new SubscriptionDelivery(
                entity.getDeliveryId(),
                entity.getEventId(),
                entity.getSubscriptionId(),
                entity.getSequenceNumber(),
                DeliveryStatus.valueOf(entity.getStatus()),
                entity.getAttemptCount(),
                entity.getNextRetryAt(),
                entity.getFinalHttpStatus(),
                entity.getErrorReason(),
                entity.getCreatedAt(),
                entity.getUpdatedAt()
        );
    }

    public DeliveryAttemptRecord toDomain(DeliveryAttemptEntity entity) {
        return new DeliveryAttemptRecord(
                entity.getAttemptId(),
                entity.getDeliveryId(),
                entity.getAttemptNumber(),
                entity.getHttpStatus(),
                entity.getErrorReason(),
                DeliveryStatus.valueOf(entity.getStatus()),
                entity.getStartedAt(),
                entity.getFinishedAt(),
                entity.getTraceId(),
                entity.getSpanId()
        );
    }

    public String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException ex) {
            throw new IllegalArgumentException("Unable to serialize JSON", ex);
        }
    }
}
