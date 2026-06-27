package com.eventdriven.notification.fanout.application.exception;

/**
 * Thrown when a subscription filter rule cannot be parsed or evaluated.
 */
public class FilterEvaluationException extends FanoutServiceException {

    public FilterEvaluationException(String message) {
        super(message);
    }

    public FilterEvaluationException(String message, Throwable cause) {
        super(message, cause);
    }
}
