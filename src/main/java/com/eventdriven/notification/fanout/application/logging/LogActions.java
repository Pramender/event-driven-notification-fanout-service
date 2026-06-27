package com.eventdriven.notification.fanout.application.logging;

/**
 * Canonical action names for structured logging.
 */
public final class LogActions {

    public static final String EVENT_ACCEPT = "event.accept";
    public static final String EVENT_INGEST = "kafka.event.ingest";
    public static final String FANOUT_MATCH = "fanout.match";
    public static final String FANOUT_COMPLETE = "fanout.complete";
    public static final String DELIVERY_DISPATCH = "delivery.dispatch";
    public static final String DELIVERY_SCHEDULER = "delivery.scheduler";
    public static final String WEBHOOK_REQUEST = "webhook.request";
    public static final String HTTP_ERROR = "http.error";

    private LogActions() {
    }
}
