package com.eventdriven.notification.fanout.domain;

/**
 * Exponential backoff retry configuration for webhook delivery.
 */
public record RetryPolicy(
        int maxAttempts,
        long initialBackoffMs,
        long maxBackoffMs,
        double multiplier
) {
    public RetryPolicy {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (initialBackoffMs < 0) {
            initialBackoffMs = 1000;
        }
        if (maxBackoffMs < initialBackoffMs) {
            maxBackoffMs = initialBackoffMs;
        }
        if (multiplier < 1.0) {
            multiplier = 2.0;
        }
    }

    /**
     * Computes backoff with full jitter: random value in [0, capped exponential delay].
     */
    public long computeBackoffMs(int attemptNumber) {
        double exponential = initialBackoffMs * Math.pow(multiplier, Math.max(0, attemptNumber - 1));
        long capped = (long) Math.min(exponential, maxBackoffMs);
        return (long) (Math.random() * capped);
    }
}
