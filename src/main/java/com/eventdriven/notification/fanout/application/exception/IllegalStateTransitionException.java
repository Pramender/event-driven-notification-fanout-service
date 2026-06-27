package com.eventdriven.notification.fanout.application.exception;

/**
 * Thrown when an illegal delivery status transition is attempted.
 */
public class IllegalStateTransitionException extends FanoutServiceException {

    public IllegalStateTransitionException(String message) {
        super(message);
    }
}
