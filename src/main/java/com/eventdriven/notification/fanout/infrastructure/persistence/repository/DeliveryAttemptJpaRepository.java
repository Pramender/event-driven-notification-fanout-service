package com.eventdriven.notification.fanout.infrastructure.persistence.repository;

import com.eventdriven.notification.fanout.infrastructure.persistence.entity.DeliveryAttemptEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface DeliveryAttemptJpaRepository extends JpaRepository<DeliveryAttemptEntity, UUID> {

    List<DeliveryAttemptEntity> findByDeliveryIdOrderByAttemptNumberAsc(UUID deliveryId);
}
