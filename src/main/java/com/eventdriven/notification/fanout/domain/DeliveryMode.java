package com.eventdriven.notification.fanout.domain;

/**
 * Delivery guarantee mode configured per subscription.
 */
public enum DeliveryMode {
    /** May deliver duplicates; retries until success or max attempts. */
    AT_LEAST_ONCE,
    /** Never redeliver after first successful delivery. */
    AT_MOST_ONCE
}
