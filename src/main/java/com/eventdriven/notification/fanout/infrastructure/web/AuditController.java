package com.eventdriven.notification.fanout.infrastructure.web;

import com.eventdriven.notification.fanout.application.audit.AuditService;
import com.eventdriven.notification.fanout.application.audit.DeliveryAuditView;
import com.eventdriven.notification.fanout.domain.DeliveryAttemptRecord;
import com.eventdriven.notification.fanout.domain.DeliveryStatus;
import com.eventdriven.notification.fanout.infrastructure.web.dto.DeliveryAuditResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/v1/audit")
public class AuditController {

    private final AuditService auditService;

    public AuditController(AuditService auditService) {
        this.auditService = auditService;
    }

    @GetMapping("/events/{eventId}")
    public List<DeliveryAuditResponse> byEvent(@PathVariable UUID eventId) {
        return auditService.byEvent(eventId).stream().map(this::toResponse).toList();
    }

    @GetMapping("/subscriptions/{subscriptionId}")
    public List<DeliveryAuditResponse> bySubscription(
            @PathVariable UUID subscriptionId,
            @RequestParam(required = false) DeliveryStatus status) {
        return auditService.bySubscription(subscriptionId, status).stream().map(this::toResponse).toList();
    }

    @GetMapping("/deliveries/{deliveryId}")
    public DeliveryAuditResponse byDelivery(@PathVariable UUID deliveryId) {
        return toResponse(auditService.byDeliveryId(deliveryId));
    }

    private DeliveryAuditResponse toResponse(DeliveryAuditView view) {
        List<DeliveryAuditResponse.AttemptResponse> attempts = view.attempts().stream()
                .map(this::toAttempt)
                .toList();
        return new DeliveryAuditResponse(
                view.eventId(),
                view.subscriptionId(),
                view.deliveryId(),
                view.sequenceNumber(),
                view.finalStatus(),
                attempts
        );
    }

    private DeliveryAuditResponse.AttemptResponse toAttempt(DeliveryAttemptRecord record) {
        return new DeliveryAuditResponse.AttemptResponse(
                record.attemptNumber(),
                record.finishedAt(),
                record.httpStatus(),
                record.errorReason(),
                record.status()
        );
    }
}
