package com.eventdriven.notification.fanout.application.logging;

import org.slf4j.MDC;

import java.util.UUID;

/**
 * Populates SLF4J MDC with correlation identifiers for structured logging.
 */
public final class MdcContext {

    public static final String TRACE_ID = "traceId";
    public static final String EVENT_ID = "eventId";
    public static final String SUBSCRIPTION_ID = "subscriptionId";
    public static final String DELIVERY_ID = "deliveryId";
    public static final String SEQUENCE_NUMBER = "sequenceNumber";

    private MdcContext() {
    }

    public static void putEvent(UUID eventId, String traceId) {
        if (traceId != null) {
            MDC.put(TRACE_ID, traceId);
        }
        if (eventId != null) {
            MDC.put(EVENT_ID, eventId.toString());
        }
    }

    public static void putDelivery(UUID deliveryId, UUID subscriptionId, Long sequenceNumber) {
        if (deliveryId != null) {
            MDC.put(DELIVERY_ID, deliveryId.toString());
        }
        if (subscriptionId != null) {
            MDC.put(SUBSCRIPTION_ID, subscriptionId.toString());
        }
        if (sequenceNumber != null) {
            MDC.put(SEQUENCE_NUMBER, sequenceNumber.toString());
        }
    }

    public static void clear() {
        MDC.clear();
    }
}
