package com.eventdriven.notification.fanout.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * Immutable record of a single HTTP delivery attempt (audit trail).
 */
public record DeliveryAttemptRecord(
        UUID attemptId,
        UUID deliveryId,
        int attemptNumber,
        Integer httpStatus,
        String errorReason,
        DeliveryStatus status,
        Instant startedAt,
        Instant finishedAt,
        String traceId,
        String spanId
) {
}
