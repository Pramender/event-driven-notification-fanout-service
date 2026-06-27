package com.eventdriven.notification.fanout.application.logging;

import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Emits logs with consistent {@code action} and {@code status} fields via MDC,
 * so Logback can render them in structured JSON or a readable console format.
 */
public final class StructuredLog {

    private final Logger logger;
    private Level level = Level.INFO;
    private String action;
    private LogStatus status;
    private String message;
    private final Map<String, String> fields = new LinkedHashMap<>();
    private Throwable error;

    private StructuredLog(Logger logger) {
        this.logger = logger;
    }

    public static StructuredLog at(Logger logger) {
        return new StructuredLog(logger);
    }

    public StructuredLog level(Level level) {
        this.level = level;
        return this;
    }

    public StructuredLog action(String action) {
        this.action = action;
        return this;
    }

    public StructuredLog status(LogStatus status) {
        this.status = status;
        return this;
    }

    public StructuredLog message(String message) {
        this.message = message;
        return this;
    }

    public StructuredLog field(String key, Object value) {
        if (value != null) {
            fields.put(key, String.valueOf(value));
        }
        return this;
    }

    public StructuredLog error(Throwable error) {
        this.error = error;
        return this;
    }

    public void log() {
        Map<String, String> previous = MdcContext.snapshot();
        try {
            MdcContext.putOperation(action, status != null ? status.name() : null);
            fields.forEach(MdcContext::putField);
            if (!fields.isEmpty()) {
                String details = fields.entrySet().stream()
                        .map(entry -> entry.getKey() + "=" + entry.getValue())
                        .collect(Collectors.joining(" "));
                MdcContext.putField("details", details);
            }
            String msg = message != null ? message : fallbackMessage();
            switch (level) {
                case ERROR -> {
                    if (error != null) {
                        logger.error(msg, error);
                    } else {
                        logger.error(msg);
                    }
                }
                case WARN -> logger.warn(msg);
                case DEBUG -> logger.debug(msg);
                default -> logger.info(msg);
            }
        } finally {
            MdcContext.restore(previous);
        }
    }

    private String fallbackMessage() {
        if (action == null && status == null) {
            return "";
        }
        if (action != null && status != null) {
            return action + " " + status.name().toLowerCase().replace('_', ' ');
        }
        return action != null ? action : status.name();
    }
}
