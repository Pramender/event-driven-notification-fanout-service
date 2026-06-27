package com.eventdriven.notification.fanout.application.logging;

/**
 * Normalized outcome for structured log entries.
 */
public enum LogStatus {
    SUCCESS,
    FAILED,
    RETRY,
    SKIPPED,
    VALIDATION_FAILED,
    DLQ,
    DUPLICATE
}
