package com.eventdriven.notification.fanout.application.ingest;

import com.eventdriven.notification.fanout.application.exception.EventValidationException;
import com.eventdriven.notification.fanout.application.fanout.FanoutService;
import com.eventdriven.notification.fanout.application.metrics.FanoutMetrics;
import com.eventdriven.notification.fanout.domain.InboundEvent;
import com.eventdriven.notification.fanout.infrastructure.persistence.EntityMapper;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.EventEntity;
import com.eventdriven.notification.fanout.infrastructure.persistence.repository.EventJpaRepository;
import com.eventdriven.notification.fanout.infrastructure.web.dto.AcceptEventRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class EventIngestServiceTest {

    @Mock EventJpaRepository eventRepository;
    @Mock FanoutService fanoutService;
    @Mock FanoutMetrics metrics;
    @Mock Tracer tracer;
    @Mock Span span;
    @Mock TraceContext traceContext;

    EventIngestService service;
    ObjectMapper objectMapper = new ObjectMapper();
    EntityMapper mapper = new EntityMapper(objectMapper);

    @BeforeEach
    void setUp() {
        service = new EventIngestService(eventRepository, fanoutService, mapper, objectMapper, metrics, tracer);
        when(tracer.nextSpan()).thenReturn(span);
        when(span.name(any())).thenReturn(span);
        when(span.start()).thenReturn(span);
        when(span.context()).thenReturn(traceContext);
        when(traceContext.traceId()).thenReturn("trace-1");
        when(tracer.withSpan(any())).thenReturn(() -> {});
    }

    @Test
    void acceptsValidJsonEvent() {
        when(eventRepository.findById(any())).thenReturn(Optional.empty());
        when(eventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        InboundEvent event = service.acceptEvent("""
                {
                  "type": "order.created",
                  "source": "orders-api",
                  "payload": { "amount": 100 }
                }
                """);

        assertThat(event.type()).isEqualTo("order.created");
        assertThat(event.source()).isEqualTo("orders-api");
        verify(fanoutService).fanout(any());
        verify(metrics).eventAccepted("order.created", "orders-api");
    }

    @Test
    void returnsExistingEventOnDuplicateId() {
        UUID eventId = UUID.randomUUID();
        EventEntity existing = new EventEntity();
        existing.setEventId(eventId);
        existing.setEventType("order.created");
        existing.setSource("orders-api");
        existing.setPayload("{}");
        existing.setReceivedAt(Instant.now());
        when(eventRepository.findById(eventId)).thenReturn(Optional.of(existing));

        InboundEvent event = service.acceptEvent("""
                {
                  "event_id": "%s",
                  "type": "order.created",
                  "source": "orders-api",
                  "payload": { "amount": 100 }
                }
                """.formatted(eventId));

        assertThat(event.eventId()).isEqualTo(eventId);
        verify(eventRepository, never()).save(any());
        verify(fanoutService, never()).fanout(any());
    }

    @Test
    void rejectsInvalidJson() {
        assertThatThrownBy(() -> service.acceptEvent("{not-json"))
                .isInstanceOf(EventValidationException.class)
                .hasMessageContaining("Invalid JSON payload");
        verify(metrics).eventInvalid("invalid_json");
        verifyNoInteractions(fanoutService);
    }

    @Test
    void rejectsMissingPayload() {
        assertThatThrownBy(() -> service.acceptEvent("""
                { "type": "order.created", "source": "orders-api" }
                """))
                .isInstanceOf(EventValidationException.class)
                .hasMessageContaining("payload");
        verify(metrics).eventInvalid("missing_payload");
    }

    @Test
    void rejectsNonObjectPayload() {
        assertThatThrownBy(() -> service.acceptEvent("""
                { "type": "order.created", "source": "orders-api", "payload": "string" }
                """))
                .isInstanceOf(EventValidationException.class)
                .hasMessageContaining("payload");
    }

    @Test
    void rejectsMissingType() {
        assertThatThrownBy(() -> service.acceptEvent("""
                { "source": "orders-api", "payload": {} }
                """))
                .isInstanceOf(EventValidationException.class)
                .hasMessageContaining("type");
        verify(metrics).eventInvalid("missing_type");
    }

    @Test
    void rejectsBlankSource() {
        assertThatThrownBy(() -> service.acceptEvent("""
                { "type": "order.created", "source": "  ", "payload": {} }
                """))
                .isInstanceOf(EventValidationException.class)
                .hasMessageContaining("source");
        verify(metrics).eventInvalid("missing_source");
    }

    @Test
    void rejectsInvalidEventId() {
        assertThatThrownBy(() -> service.acceptEvent("""
                {
                  "event_id": "not-a-uuid",
                  "type": "order.created",
                  "source": "orders-api",
                  "payload": {}
                }
                """))
                .isInstanceOf(EventValidationException.class)
                .hasMessageContaining("Invalid event_id");
        verify(metrics).eventInvalid("invalid_event_id");
    }

    @Test
    void acceptsTypedRequestAndPersists() {
        when(eventRepository.findById(any())).thenReturn(Optional.empty());
        when(eventRepository.save(any())).thenAnswer(inv -> inv.getArgument(0));

        ObjectNode payload = objectMapper.createObjectNode().put("n", 1);
        UUID eventId = UUID.randomUUID();
        AcceptEventRequest request = new AcceptEventRequest(
                eventId, "order.created", "orders-api", Instant.parse("2026-06-27T10:00:00Z"), payload);

        InboundEvent event = service.acceptEvent(request);

        assertThat(event.eventId()).isEqualTo(eventId);
        ArgumentCaptor<EventEntity> captor = ArgumentCaptor.forClass(EventEntity.class);
        verify(eventRepository).save(captor.capture());
        assertThat(captor.getValue().getEventType()).isEqualTo("order.created");
        verify(fanoutService).fanout(any());
    }
}
