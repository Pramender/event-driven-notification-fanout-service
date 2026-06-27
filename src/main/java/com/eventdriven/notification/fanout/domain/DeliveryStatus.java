package com.eventdriven.notification.fanout.domain;

/**
 * Lifecycle states for a subscription delivery (event × subscriber pair).
 * Transitions are enforced by {@link com.eventdriven.notification.fanout.application.delivery.DeliveryStateMachine}.
 */
public enum DeliveryStatus {
    /** Matched and waiting for head-of-line slot. */
    QUEUED,
    /** Currently being delivered over HTTP. */
    IN_FLIGHT,
    /** Successfully delivered (terminal). */
    SENT,
    /** Transient failure; scheduled for retry. */
    RETRY_PENDING,
    /** Permanent failure or max retries exceeded (terminal). */
    FAILED
}
