package com.eventdriven.notification.fanout.infrastructure.web.dto;

import com.eventdriven.notification.fanout.domain.DeliveryStatus;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record DeliveryAuditResponse(
        UUID eventId,
        UUID subscriptionId,
        UUID deliveryId,
        long sequenceNumber,
        DeliveryStatus finalStatus,
        List<AttemptResponse> attempts
) {
    public record AttemptResponse(
            int attemptNumber,
            Instant timestamp,
            Integer httpStatus,
            String errorReason,
            DeliveryStatus status
    ) {
    }
}
