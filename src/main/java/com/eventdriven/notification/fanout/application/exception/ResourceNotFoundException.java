package com.eventdriven.notification.fanout.application.exception;

import java.util.UUID;

/**
 * Thrown when a requested entity does not exist.
 */
public class ResourceNotFoundException extends FanoutServiceException {

    public ResourceNotFoundException(String resource, UUID id) {
        super(resource + " not found: " + id);
    }

    public ResourceNotFoundException(String message) {
        super(message);
    }
}
