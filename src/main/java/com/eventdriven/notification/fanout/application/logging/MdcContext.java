package com.eventdriven.notification.fanout.application.logging;

import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Populates SLF4J MDC with correlation identifiers and structured log fields.
 */
public final class MdcContext {

    public static final String TRACE_ID = "traceId";
    public static final String EVENT_ID = "eventId";
    public static final String SUBSCRIPTION_ID = "subscriptionId";
    public static final String DELIVERY_ID = "deliveryId";
    public static final String SEQUENCE_NUMBER = "sequenceNumber";
    public static final String ACTION = "action";
    public static final String STATUS = "status";

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

    public static void putOperation(String action, String status) {
        if (action != null) {
            MDC.put(ACTION, action);
        }
        if (status != null) {
            MDC.put(STATUS, status);
        }
    }

    public static void putField(String key, String value) {
        if (key != null && value != null) {
            MDC.put(key, value);
        }
    }

    public static Map<String, String> snapshot() {
        Map<String, String> context = MDC.getCopyOfContextMap();
        return context != null ? new HashMap<>(context) : Map.of();
    }

    public static void restore(Map<String, String> snapshot) {
        if (snapshot == null || snapshot.isEmpty()) {
            MDC.clear();
        } else {
            MDC.setContextMap(snapshot);
        }
    }

    public static void clear() {
        MDC.clear();
    }
}
