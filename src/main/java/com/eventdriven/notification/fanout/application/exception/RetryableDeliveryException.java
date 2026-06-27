package com.eventdriven.notification.fanout.application.exception;

/**
 * Transient webhook failure (5xx, 429, timeout) eligible for retry.
 */
public class RetryableDeliveryException extends FanoutServiceException {

    private final Integer httpStatus;

    public RetryableDeliveryException(String message, Integer httpStatus) {
        super(message);
        this.httpStatus = httpStatus;
    }

    public RetryableDeliveryException(String message, Integer httpStatus, Throwable cause) {
        super(message, cause);
        this.httpStatus = httpStatus;
    }

    public Integer getHttpStatus() {
        return httpStatus;
    }
}
