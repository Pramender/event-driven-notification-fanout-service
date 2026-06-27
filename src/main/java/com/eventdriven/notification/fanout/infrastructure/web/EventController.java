package com.eventdriven.notification.fanout.infrastructure.web;

import com.eventdriven.notification.fanout.application.ingest.EventIngestService;
import com.eventdriven.notification.fanout.domain.InboundEvent;
import com.eventdriven.notification.fanout.infrastructure.web.dto.AcceptEventRequest;
import com.eventdriven.notification.fanout.infrastructure.web.dto.AcceptEventResponse;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/v1/events")
public class EventController {

    private final EventIngestService ingestService;

    public EventController(EventIngestService ingestService) {
        this.ingestService = ingestService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public AcceptEventResponse accept(@Valid @RequestBody AcceptEventRequest request) {
        InboundEvent event = ingestService.acceptEvent(request);
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
