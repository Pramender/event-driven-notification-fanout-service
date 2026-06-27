package com.eventdriven.notification.fanout.infrastructure.web;

import com.eventdriven.notification.fanout.application.ingest.EventIngestService;
import com.eventdriven.notification.fanout.domain.InboundEvent;
import com.eventdriven.notification.fanout.infrastructure.web.dto.AcceptEventRequest;
import com.eventdriven.notification.fanout.infrastructure.web.dto.AcceptEventResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/v1/events")
public class EventController {

    private final EventIngestService ingestService;
    private final ObjectMapper objectMapper;

    public EventController(EventIngestService ingestService, ObjectMapper objectMapper) {
        this.ingestService = ingestService;
        this.objectMapper = objectMapper;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public AcceptEventResponse accept(@Valid @RequestBody AcceptEventRequest request) throws JsonProcessingException {
        InboundEvent event = ingestService.acceptEvent(objectMapper.writeValueAsString(request));
        return toResponse(event);
    }

    private AcceptEventResponse toResponse(InboundEvent event) {
        return new AcceptEventResponse(
                event.eventId(),
                event.type(),
                event.source(),
                event.payload(),
                event.occurredAt(),
                event.receivedAt()
        );
    }
}
