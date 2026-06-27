package com.eventdriven.notification.fanout.infrastructure.persistence.repository;

import com.eventdriven.notification.fanout.infrastructure.persistence.entity.SubscriptionDeliveryCursorEntity;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface SubscriptionDeliveryCursorJpaRepository extends JpaRepository<SubscriptionDeliveryCursorEntity, UUID> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT c FROM SubscriptionDeliveryCursorEntity c WHERE c.subscriptionId = :subscriptionId")
    Optional<SubscriptionDeliveryCursorEntity> findForUpdate(@Param("subscriptionId") UUID subscriptionId);
}
