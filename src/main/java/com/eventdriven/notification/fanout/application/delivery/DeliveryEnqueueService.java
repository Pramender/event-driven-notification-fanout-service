package com.eventdriven.notification.fanout.application.delivery;

import com.eventdriven.notification.fanout.domain.DeliveryStatus;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.SubscriptionDeliveryCursorEntity;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.SubscriptionDeliveryEntity;
import com.eventdriven.notification.fanout.infrastructure.persistence.repository.SubscriptionDeliveryCursorJpaRepository;
import com.eventdriven.notification.fanout.infrastructure.persistence.repository.SubscriptionDeliveryJpaRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Enqueues subscription deliveries at the tail of the per-subscription FIFO queue.
 */
@Service
public class DeliveryEnqueueService {

    private final SubscriptionDeliveryJpaRepository deliveryRepository;
    private final SubscriptionDeliveryCursorJpaRepository cursorRepository;

    public DeliveryEnqueueService(
            SubscriptionDeliveryJpaRepository deliveryRepository,
            SubscriptionDeliveryCursorJpaRepository cursorRepository) {
        this.deliveryRepository = deliveryRepository;
        this.cursorRepository = cursorRepository;
    }

    @Transactional
    public SubscriptionDeliveryEntity enqueue(UUID subscriptionId, UUID eventId) {
        SubscriptionDeliveryCursorEntity cursor = lockCursor(subscriptionId);
        long sequenceNumber = cursor.getNextAssignSeq();
        cursor.setNextAssignSeq(sequenceNumber + 1);
        cursor.setUpdatedAt(Instant.now());
        cursorRepository.save(cursor);

        Instant now = Instant.now();
        SubscriptionDeliveryEntity delivery = new SubscriptionDeliveryEntity();
        delivery.setDeliveryId(UUID.randomUUID());
        delivery.setEventId(eventId);
        delivery.setSubscriptionId(subscriptionId);
        delivery.setSequenceNumber(sequenceNumber);
        delivery.setStatus(DeliveryStatus.QUEUED.name());
        delivery.setAttemptCount(0);
        delivery.setCreatedAt(now);
        delivery.setUpdatedAt(now);
        return deliveryRepository.save(delivery);
    }

    @Transactional
    public SubscriptionDeliveryEntity requeue(SubscriptionDeliveryEntity delivery) {
        SubscriptionDeliveryCursorEntity cursor = lockCursor(delivery.getSubscriptionId());
        long sequenceNumber = cursor.getNextAssignSeq();
        cursor.setNextAssignSeq(sequenceNumber + 1);
        cursor.setUpdatedAt(Instant.now());
        cursorRepository.save(cursor);

        delivery.setSequenceNumber(sequenceNumber);
        delivery.setStatus(DeliveryStatus.QUEUED.name());
        delivery.setAttemptCount(0);
        delivery.setNextRetryAt(null);
        delivery.setFinalHttpStatus(null);
        delivery.setErrorReason(null);
        delivery.setUpdatedAt(Instant.now());
        return deliveryRepository.save(delivery);
    }

    private SubscriptionDeliveryCursorEntity lockCursor(UUID subscriptionId) {
        return cursorRepository.findForUpdate(subscriptionId)
                .orElseGet(() -> initializeCursor(subscriptionId));
    }

    private SubscriptionDeliveryCursorEntity initializeCursor(UUID subscriptionId) {
        SubscriptionDeliveryCursorEntity cursor = new SubscriptionDeliveryCursorEntity();
        cursor.setSubscriptionId(subscriptionId);
        cursor.setNextDeliverableSeq(1);
        cursor.setNextAssignSeq(1);
        cursor.setUpdatedAt(Instant.now());
        return cursorRepository.save(cursor);
    }
}
