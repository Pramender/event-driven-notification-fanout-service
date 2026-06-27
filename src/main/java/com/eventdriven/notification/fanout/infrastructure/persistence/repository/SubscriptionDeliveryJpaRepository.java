package com.eventdriven.notification.fanout.infrastructure.persistence.repository;

import com.eventdriven.notification.fanout.infrastructure.persistence.entity.SubscriptionDeliveryEntity;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface SubscriptionDeliveryJpaRepository extends JpaRepository<SubscriptionDeliveryEntity, UUID> {

    List<SubscriptionDeliveryEntity> findByEventId(UUID eventId);

    List<SubscriptionDeliveryEntity> findBySubscriptionId(UUID subscriptionId);

    List<SubscriptionDeliveryEntity> findBySubscriptionIdAndStatus(UUID subscriptionId, String status);

    @Query("""
            SELECT d FROM SubscriptionDeliveryEntity d
            WHERE d.subscriptionId = :subscriptionId
              AND d.sequenceNumber = :sequenceNumber
            """)
    Optional<SubscriptionDeliveryEntity> findBySubscriptionAndSequence(
            @Param("subscriptionId") UUID subscriptionId,
            @Param("sequenceNumber") long sequenceNumber);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT d FROM SubscriptionDeliveryEntity d WHERE d.deliveryId = :deliveryId")
    Optional<SubscriptionDeliveryEntity> findForUpdate(@Param("deliveryId") UUID deliveryId);

    @Query(value = """
            SELECT d.* FROM subscription_deliveries d
            JOIN subscription_delivery_cursors c ON c.subscription_id = d.subscription_id
            WHERE d.status IN ('QUEUED', 'RETRY_PENDING')
              AND d.sequence_number = c.next_deliverable_seq
              AND (d.next_retry_at IS NULL OR d.next_retry_at <= :now)
            ORDER BY d.created_at
            LIMIT :limit
            FOR UPDATE OF d SKIP LOCKED
            """, nativeQuery = true)
    List<SubscriptionDeliveryEntity> findReadyForDispatch(@Param("now") Instant now, @Param("limit") int limit);

    boolean existsByEventIdAndSubscriptionIdAndStatus(UUID eventId, UUID subscriptionId, String status);
}
