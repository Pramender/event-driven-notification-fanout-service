package com.eventdriven.notification.fanout.infrastructure.persistence.repository;

import com.eventdriven.notification.fanout.infrastructure.persistence.entity.EventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface EventJpaRepository extends JpaRepository<EventEntity, UUID> {

    @Query("""
            SELECT e FROM EventEntity e
            WHERE COALESCE(e.occurredAt, e.receivedAt) >= :from
              AND COALESCE(e.occurredAt, e.receivedAt) <= :to
            ORDER BY COALESCE(e.occurredAt, e.receivedAt) ASC, e.receivedAt ASC
            """)
    List<EventEntity> findByEffectiveTimestampBetween(
            @Param("from") Instant from,
            @Param("to") Instant to);
}
