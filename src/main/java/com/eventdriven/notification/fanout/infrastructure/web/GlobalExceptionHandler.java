package com.eventdriven.notification.fanout.infrastructure.web;

import com.eventdriven.notification.fanout.application.exception.*;
import com.eventdriven.notification.fanout.application.logging.LogActions;
import com.eventdriven.notification.fanout.application.logging.LogStatus;
import com.eventdriven.notification.fanout.application.logging.StructuredLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.stream.Collectors;

/**
 * Maps application exceptions to RFC 7807 {@link ProblemDetail} responses.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ResourceNotFoundException.class)
    public ProblemDetail handleNotFound(ResourceNotFoundException ex) {
        ProblemDetail detail = ProblemDetail.forStatus(HttpStatus.NOT_FOUND);
        detail.setTitle("Resource not found");
        detail.setDetail(ex.getMessage());
        return detail;
    }

    @ExceptionHandler(EventValidationException.class)
    public ProblemDetail handleValidation(EventValidationException ex) {
        StructuredLog.at(log)
                .level(Level.WARN)
                .action(LogActions.HTTP_ERROR)
                .status(LogStatus.VALIDATION_FAILED)
                .field("reason", ex.getMessage())
                .message("Event validation failed")
                .log();
        ProblemDetail detail = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        detail.setTitle("Invalid event");
        detail.setDetail(ex.getMessage());
        return detail;
    }

    @ExceptionHandler(FilterEvaluationException.class)
    public ProblemDetail handleFilter(FilterEvaluationException ex) {
        ProblemDetail detail = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        detail.setTitle("Invalid subscription filter");
        detail.setDetail(ex.getMessage());
        return detail;
    }

    @ExceptionHandler(ReplayValidationException.class)
    public ProblemDetail handleReplay(ReplayValidationException ex) {
        ProblemDetail detail = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        detail.setTitle("Invalid replay request");
        detail.setDetail(ex.getMessage());
        return detail;
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleBeanValidation(MethodArgumentNotValidException ex) {
        String fieldErrors = ex.getBindingResult().getFieldErrors().stream()
                .map(this::formatFieldError)
                .collect(Collectors.joining("; "));
        ProblemDetail detail = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        detail.setTitle("Validation failed");
        detail.setDetail(fieldErrors.isBlank() ? "Request validation failed" : fieldErrors);
        return detail;
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ProblemDetail handleUnreadableBody(HttpMessageNotReadableException ex) {
        ProblemDetail detail = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        detail.setTitle("Invalid request body");
        detail.setDetail(extractReadableCause(ex));
        return detail;
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail handleIllegalArgument(IllegalArgumentException ex) {
        ProblemDetail detail = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        detail.setTitle("Invalid request");
        detail.setDetail(ex.getMessage());
        return detail;
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ProblemDetail handleTypeMismatch(MethodArgumentTypeMismatchException ex) {
        ProblemDetail detail = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        detail.setTitle("Invalid request parameter");
        detail.setDetail("Invalid value for parameter '" + ex.getName() + "': " + ex.getValue());
        return detail;
    }

    @ExceptionHandler(FanoutServiceException.class)
    public ProblemDetail handleDomain(FanoutServiceException ex) {
        StructuredLog.at(log)
                .level(Level.ERROR)
                .action(LogActions.HTTP_ERROR)
                .status(LogStatus.FAILED)
                .field("reason", ex.getMessage())
                .message("Request processing failed")
                .error(ex)
                .log();
        ProblemDetail detail = ProblemDetail.forStatus(HttpStatus.UNPROCESSABLE_ENTITY);
        detail.setTitle("Processing error");
        detail.setDetail(ex.getMessage());
        return detail;
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleGeneric(Exception ex) {
        StructuredLog.at(log)
                .level(Level.ERROR)
                .action(LogActions.HTTP_ERROR)
                .status(LogStatus.FAILED)
                .field("reason", ex.getMessage())
                .message("Unexpected server error")
                .error(ex)
                .log();
        ProblemDetail detail = ProblemDetail.forStatus(HttpStatus.INTERNAL_SERVER_ERROR);
        detail.setTitle("Internal server error");
        detail.setDetail("An unexpected error occurred");
        return detail;
    }

    private String formatFieldError(FieldError error) {
        return error.getField() + ": " + error.getDefaultMessage();
    }

    private String extractReadableCause(HttpMessageNotReadableException ex) {
        Throwable cause = ex.getCause();
        if (cause != null && cause.getMessage() != null && !cause.getMessage().isBlank()) {
            return cause.getMessage();
        }
        String message = ex.getMessage();
        if (message != null && message.contains(":")) {
            return message.substring(message.indexOf(':') + 1).trim();
        }
        return "Malformed JSON request body";
    }
}
