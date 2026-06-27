package com.eventdriven.notification.fanout.application.exception;

/**
 * Thrown when inbound event payload fails schema validation.
 */
public class EventValidationException extends FanoutServiceException {

    public EventValidationException(String message) {
        super(message);
    }
}
