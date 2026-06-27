package com.eventdriven.notification.fanout.application.audit;

import com.eventdriven.notification.fanout.domain.DeliveryAttemptRecord;
import com.eventdriven.notification.fanout.domain.DeliveryStatus;

import java.util.List;
import java.util.UUID;

/**
 * Audit view combining delivery summary and attempt timeline.
 */
public record DeliveryAuditView(
        UUID eventId,
        UUID subscriptionId,
        UUID deliveryId,
        long sequenceNumber,
        DeliveryStatus finalStatus,
        List<DeliveryAttemptRecord> attempts
) {
}
