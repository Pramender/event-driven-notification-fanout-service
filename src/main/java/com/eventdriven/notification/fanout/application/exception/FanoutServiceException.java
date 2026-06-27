package com.eventdriven.notification.fanout.application.exception;

/**
 * Base runtime exception for domain and application errors.
 */
public class FanoutServiceException extends RuntimeException {

    public FanoutServiceException(String message) {
        super(message);
    }

    public FanoutServiceException(String message, Throwable cause) {
        super(message, cause);
    }
}
