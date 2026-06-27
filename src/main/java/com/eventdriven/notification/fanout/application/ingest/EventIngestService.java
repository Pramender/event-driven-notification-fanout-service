package com.eventdriven.notification.fanout.application.ingest;

import com.eventdriven.notification.fanout.application.exception.EventValidationException;
import com.eventdriven.notification.fanout.application.fanout.FanoutService;
import com.eventdriven.notification.fanout.application.metrics.FanoutMetrics;
import com.eventdriven.notification.fanout.domain.InboundEvent;
import com.eventdriven.notification.fanout.infrastructure.persistence.EntityMapper;
import com.eventdriven.notification.fanout.infrastructure.persistence.entity.EventEntity;
import com.eventdriven.notification.fanout.infrastructure.persistence.repository.EventJpaRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Persists accepted inbound events then triggers fanout.
 * Idempotent on {@code event_id} — duplicate Kafka deliveries are safe.
 */
@Service
public class EventIngestService {

    private static final Logger log = LoggerFactory.getLogger(EventIngestService.class);

    private final EventJpaRepository eventRepository;
    private final FanoutService fanoutService;
    private final EntityMapper mapper;
    private final ObjectMapper objectMapper;
    private final FanoutMetrics metrics;
    private final Tracer tracer;

    public EventIngestService(
            EventJpaRepository eventRepository,
            FanoutService fanoutService,
            EntityMapper mapper,
            ObjectMapper objectMapper,
            FanoutMetrics metrics,
            Tracer tracer) {
        this.eventRepository = eventRepository;
        this.fanoutService = fanoutService;
        this.mapper = mapper;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
        this.tracer = tracer;
    }

    @Transactional
    public InboundEvent acceptEvent(String rawJson) {
        Span span = tracer.nextSpan().name("event.accept").start();
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            JsonNode root = parseJson(rawJson);
            InboundEvent event = validateAndBuild(root);
            Optional<EventEntity> existing = eventRepository.findById(event.eventId());
            if (existing.isPresent()) {
                log.info("Duplicate event ingest ignored for eventId={}", event.eventId());
                return mapper.toDomain(existing.get());
            }

            EventEntity entity = new EventEntity();
            entity.setEventId(event.eventId());
            entity.setEventType(event.type());
            entity.setSource(event.source());
            entity.setPayload(mapper.writeJson(event.payload()));
            entity.setOccurredAt(event.occurredAt());
            entity.setReceivedAt(event.receivedAt());
            entity.setTraceId(span.context().traceId());
            eventRepository.save(entity);

            metrics.eventAccepted(event.type(), event.source());
            log.info("Event accepted type={} source={} eventId={}", event.type(), event.source(), event.eventId());

            InboundEvent persisted = mapper.toDomain(entity);
            fanoutService.fanout(persisted);
            return persisted;
        } finally {
            span.end();
        }
    }

    private JsonNode parseJson(String rawJson) {
        try {
            return objectMapper.readTree(rawJson);
        } catch (Exception ex) {
            metrics.eventInvalid("invalid_json");
            throw new EventValidationException("Invalid JSON payload: " + ex.getMessage());
        }
    }

    private InboundEvent validateAndBuild(JsonNode root) {
        String type = requiredText(root, "type");
        String source = requiredText(root, "source");
        JsonNode payload = root.get("payload");
        if (payload == null || !payload.isObject()) {
            metrics.eventInvalid("missing_payload");
            throw new EventValidationException("Field 'payload' must be a JSON object");
        }

        UUID eventId = root.hasNonNull("event_id")
                ? UUID.fromString(root.get("event_id").asText())
                : UUID.randomUUID();

        Instant occurredAt = root.hasNonNull("occurred_at")
                ? Instant.parse(root.get("occurred_at").asText())
                : null;

        return new InboundEvent(
                eventId,
                type,
                source,
                payload,
                occurredAt,
                Instant.now(),
                null
        );
    }

    private String requiredText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual() || value.asText().isBlank()) {
            metrics.eventInvalid("missing_" + field);
            throw new EventValidationException("Required field missing or blank: " + field);
        }
        return value.asText();
    }
}
