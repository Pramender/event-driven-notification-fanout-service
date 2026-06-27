package com.eventdriven.notification.fanout.application.exception;

/**
 * Thrown when a replay request has invalid parameters.
 */
public class ReplayValidationException extends FanoutServiceException {

    public ReplayValidationException(String message) {
        super(message);
    }
}
