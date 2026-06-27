package com.eventdriven.notification.fanout.application.subscription;

import com.eventdriven.notification.fanout.application.exception.ResourceNotFoundException;
import com.eventdriven.notification.fanout.domain.DeliveryMode;
import com.eventdriven.notification.fanout.domain.RetryPolicy;
import com.eventdriven.notification.fanout.domain.Subscription;
import com.eventdriven.notification.fanout.domain.WebhookTarget;
import com.eventdriven.notification.fanout.infrastructure.persistence.EntityMapper;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.SubscriptionDeliveryCursorEntity;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.SubscriptionEntity;
import com.eventdriven.notification.fanout.infrastructure.persistence.repository.SubscriptionDeliveryCursorJpaRepository;
import com.eventdriven.notification.fanout.infrastructure.persistence.repository.SubscriptionJpaRepository;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * CRUD operations for subscriptions with hot cache invalidation (no service restart).
 */
@Service
public class SubscriptionService {

    private final SubscriptionJpaRepository repository;
    private final SubscriptionDeliveryCursorJpaRepository cursorRepository;
    private final SubscriptionCache cache;
    private final EntityMapper mapper;

    public SubscriptionService(
            SubscriptionJpaRepository repository,
            SubscriptionDeliveryCursorJpaRepository cursorRepository,
            SubscriptionCache cache,
            EntityMapper mapper) {
        this.repository = repository;
        this.cursorRepository = cursorRepository;
        this.cache = cache;
        this.mapper = mapper;
    }

    @Transactional
    public Subscription create(
            String name,
            DeliveryMode deliveryMode,
            JsonNode filter,
            WebhookTarget target,
            RetryPolicy retryPolicy) {
        UUID id = UUID.randomUUID();
        Instant now = Instant.now();

        SubscriptionEntity entity = new SubscriptionEntity();
        entity.setSubscriptionId(id);
        entity.setName(name);
        entity.setEnabled(true);
        entity.setDeliveryMode(deliveryMode.name());
        entity.setFilterJson(mapper.writeJson(filter));
        entity.setTargetJson(mapper.writeJson(target));
        entity.setRetryPolicyJson(mapper.writeJson(retryPolicy));
        entity.setCreatedAt(now);
        entity.setUpdatedAt(now);
        entity.setDeleted(false);
        repository.save(entity);

        SubscriptionDeliveryCursorEntity cursor = new SubscriptionDeliveryCursorEntity();
        cursor.setSubscriptionId(id);
        cursor.setNextDeliverableSeq(1);
        cursor.setNextAssignSeq(1);
        cursor.setUpdatedAt(now);
        cursorRepository.save(cursor);

        cache.invalidate();
        return mapper.toDomain(entity);
    }

    @Transactional(readOnly = true)
    public List<Subscription> listActive() {
        return repository.findAllActive().stream().map(mapper::toDomain).toList();
    }

    @Transactional(readOnly = true)
    public Subscription get(UUID subscriptionId) {
        SubscriptionEntity entity = repository.findById(subscriptionId)
                .filter(s -> !s.isDeleted())
                .orElseThrow(() -> new ResourceNotFoundException("Subscription", subscriptionId));
        return mapper.toDomain(entity);
    }

    @Transactional
    public void delete(UUID subscriptionId) {
        SubscriptionEntity entity = repository.findById(subscriptionId)
                .orElseThrow(() -> new ResourceNotFoundException("Subscription", subscriptionId));
        entity.setDeleted(true);
        entity.setEnabled(false);
        entity.setUpdatedAt(Instant.now());
        repository.save(entity);
        cache.invalidate();
    }
}
