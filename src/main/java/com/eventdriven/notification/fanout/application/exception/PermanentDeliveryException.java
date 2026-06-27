package com.eventdriven.notification.fanout.application.exception;

/**
 * Permanent webhook failure (most 4xx) — no retry.
 */
public class PermanentDeliveryException extends FanoutServiceException {

    private final Integer httpStatus;

    public PermanentDeliveryException(String message, Integer httpStatus) {
        super(message);
        this.httpStatus = httpStatus;
    }

    public Integer getHttpStatus() {
        return httpStatus;
    }
}
