package com.eventdriven.notification.fanout.infrastructure.web;

import com.eventdriven.notification.fanout.application.exception.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GlobalExceptionHandlerTest {

    private GlobalExceptionHandler handler;

    @BeforeEach
    void setUp() {
        handler = new GlobalExceptionHandler();
    }

    @Test
    void mapsResourceNotFoundTo404() {
        UUID id = UUID.randomUUID();
        ProblemDetail detail = handler.handleNotFound(new ResourceNotFoundException("Subscription", id));

        assertThat(detail.getStatus()).isEqualTo(HttpStatus.NOT_FOUND.value());
        assertThat(detail.getTitle()).isEqualTo("Resource not found");
        assertThat(detail.getDetail()).contains(id.toString());
    }

    @Test
    void mapsEventValidationTo400() {
        ProblemDetail detail = handler.handleValidation(new EventValidationException("Field 'payload' must be a JSON object"));

        assertThat(detail.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST.value());
        assertThat(detail.getTitle()).isEqualTo("Invalid event");
        assertThat(detail.getDetail()).contains("payload");
    }

    @Test
    void mapsFilterEvaluationTo400() {
        ProblemDetail detail = handler.handleFilter(new FilterEvaluationException("Unsupported filter op: foo"));

        assertThat(detail.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST.value());
        assertThat(detail.getTitle()).isEqualTo("Invalid subscription filter");
        assertThat(detail.getDetail()).contains("Unsupported filter op");
    }

    @Test
    void mapsBeanValidationTo400WithFieldDetails() throws Exception {
        BeanPropertyBindingResult bindingResult = new BeanPropertyBindingResult(new Object(), "request");
        bindingResult.addError(new FieldError("request", "type", "must not be blank"));
        MethodParameter parameter = new MethodParameter(String.class.getDeclaredMethod("toString"), -1);
        MethodArgumentNotValidException ex = new MethodArgumentNotValidException(parameter, bindingResult);

        ProblemDetail detail = handler.handleBeanValidation(ex);

        assertThat(detail.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST.value());
        assertThat(detail.getTitle()).isEqualTo("Validation failed");
        assertThat(detail.getDetail()).contains("type: must not be blank");
    }

    @Test
    void mapsTypeMismatchTo400() {
        MethodArgumentTypeMismatchException ex = mock(MethodArgumentTypeMismatchException.class);
        when(ex.getName()).thenReturn("status");
        when(ex.getValue()).thenReturn("NOT_A_STATUS");

        ProblemDetail detail = handler.handleTypeMismatch(ex);

        assertThat(detail.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST.value());
        assertThat(detail.getDetail()).contains("status").contains("NOT_A_STATUS");
    }

    @Test
    void mapsIllegalArgumentTo400() {
        ProblemDetail detail = handler.handleIllegalArgument(new IllegalArgumentException("Webhook URL is required"));

        assertThat(detail.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST.value());
        assertThat(detail.getTitle()).isEqualTo("Invalid request");
        assertThat(detail.getDetail()).isEqualTo("Webhook URL is required");
    }

    @Test
    void mapsUnreadableBodyTo400() {
        HttpMessageNotReadableException ex = new HttpMessageNotReadableException(
                "JSON parse error: Unexpected character",
                new IllegalArgumentException("Unexpected character ('x')"));

        ProblemDetail detail = handler.handleUnreadableBody(ex);

        assertThat(detail.getStatus()).isEqualTo(HttpStatus.BAD_REQUEST.value());
        assertThat(detail.getTitle()).isEqualTo("Invalid request body");
        assertThat(detail.getDetail()).contains("Unexpected character");
    }

    @Test
    void mapsFanoutServiceExceptionTo422() {
        ProblemDetail detail = handler.handleDomain(new FanoutServiceException("Processing failed"));

        assertThat(detail.getStatus()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY.value());
        assertThat(detail.getTitle()).isEqualTo("Processing error");
        assertThat(detail.getDetail()).isEqualTo("Processing failed");
    }

    @Test
    void mapsUnexpectedExceptionTo500WithoutLeakingDetails() {
        ProblemDetail detail = handler.handleGeneric(new RuntimeException("database connection lost"));

        assertThat(detail.getStatus()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR.value());
        assertThat(detail.getTitle()).isEqualTo("Internal server error");
        assertThat(detail.getDetail()).isEqualTo("An unexpected error occurred");
    }
}
