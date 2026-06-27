package com.eventdriven.notification.fanout.infrastructure.persistence.repository;

import com.eventdriven.notification.fanout.infrastructure.persistence.entity.SubscriptionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.UUID;

public interface SubscriptionJpaRepository extends JpaRepository<SubscriptionEntity, UUID> {

    @Query("SELECT s FROM SubscriptionEntity s WHERE s.deleted = false AND s.enabled = true")
    List<SubscriptionEntity> findAllActive();
}
