package com.eventdriven.notification.fanout.application.subscription;

import com.eventdriven.notification.fanout.config.FanoutProperties;
import com.eventdriven.notification.fanout.domain.Subscription;
import com.eventdriven.notification.fanout.infrastructure.persistence.EntityMapper;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.SubscriptionEntity;
import com.eventdriven.notification.fanout.infrastructure.persistence.repository.SubscriptionJpaRepository;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory cache of active subscriptions with TTL-based refresh (no restart required for CRUD changes).
 */
@Component
public class SubscriptionCache {

    private final SubscriptionJpaRepository repository;
    private final EntityMapper mapper;
    private final long ttlSeconds;
    private volatile Instant lastRefresh = Instant.EPOCH;
    private volatile List<Subscription> cachedActive = List.of();
    private final Map<UUID, Subscription> byIdCache = new ConcurrentHashMap<>();

    public SubscriptionCache(
            SubscriptionJpaRepository repository,
            EntityMapper mapper,
            FanoutProperties properties) {
        this.repository = repository;
        this.mapper = mapper;
        this.ttlSeconds = properties.subscriptionCacheTtlSeconds();
    }

    public List<Subscription> getActiveSubscriptions() {
        refreshIfStale();
        return cachedActive;
    }

    public Optional<Subscription> getById(UUID subscriptionId) {
        refreshIfStale();
        return Optional.ofNullable(byIdCache.get(subscriptionId));
    }

    public void invalidate() {
        lastRefresh = Instant.EPOCH;
        byIdCache.clear();
    }

    private void refreshIfStale() {
        if (Instant.now().isBefore(lastRefresh.plusSeconds(ttlSeconds)) && !cachedActive.isEmpty()) {
            return;
        }
        synchronized (this) {
            if (Instant.now().isBefore(lastRefresh.plusSeconds(ttlSeconds)) && !cachedActive.isEmpty()) {
                return;
            }
            List<SubscriptionEntity> entities = repository.findAllActive();
            cachedActive = entities.stream().map(mapper::toDomain).toList();
            byIdCache.clear();
            cachedActive.forEach(s -> byIdCache.put(s.subscriptionId(), s));
            lastRefresh = Instant.now();
        }
    }
}
