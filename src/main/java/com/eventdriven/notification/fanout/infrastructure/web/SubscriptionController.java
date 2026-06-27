package com.eventdriven.notification.fanout.infrastructure.web;

import com.eventdriven.notification.fanout.application.replay.ReplayService;
import com.eventdriven.notification.fanout.application.replay.ReplayResult;
import com.eventdriven.notification.fanout.application.subscription.SubscriptionService;
import com.eventdriven.notification.fanout.domain.Subscription;
import com.eventdriven.notification.fanout.infrastructure.web.dto.CreateSubscriptionRequest;
import com.eventdriven.notification.fanout.infrastructure.web.dto.ReplayEventsRequest;
import com.eventdriven.notification.fanout.infrastructure.web.dto.ReplayEventsResponse;
import com.eventdriven.notification.fanout.infrastructure.web.dto.SubscriptionResponse;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/v1/subscriptions")
public class SubscriptionController {

    private final SubscriptionService subscriptionService;
    private final ReplayService replayService;

    public SubscriptionController(SubscriptionService subscriptionService, ReplayService replayService) {
        this.subscriptionService = subscriptionService;
        this.replayService = replayService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public SubscriptionResponse create(@Valid @RequestBody CreateSubscriptionRequest request) {
        Subscription subscription = subscriptionService.create(
                request.name(),
                request.deliveryMode(),
                request.filter(),
                request.target(),
                request.retryPolicy()
        );
        return toResponse(subscription);
    }

    @GetMapping
    public List<SubscriptionResponse> list() {
        return subscriptionService.listActive().stream().map(this::toResponse).toList();
    }

    @GetMapping("/{subscriptionId}")
    public SubscriptionResponse get(@PathVariable UUID subscriptionId) {
        return toResponse(subscriptionService.get(subscriptionId));
    }

    @DeleteMapping("/{subscriptionId}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable UUID subscriptionId) {
        subscriptionService.delete(subscriptionId);
    }

    @PostMapping("/{subscriptionId}/replay")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public ReplayEventsResponse replay(
            @PathVariable UUID subscriptionId,
            @Valid @RequestBody ReplayEventsRequest request) {
        ReplayResult result = replayService.replay(subscriptionId, request.from(), request.to());
        return new ReplayEventsResponse(
                result.subscriptionId(),
                result.from(),
                result.to(),
                result.eventsScanned(),
                result.matched(),
                result.queued(),
                result.skipped()
        );
    }

    private SubscriptionResponse toResponse(Subscription subscription) {
        return new SubscriptionResponse(
                subscription.subscriptionId(),
                subscription.name(),
                subscription.enabled(),
                subscription.deliveryMode(),
                subscription.filter(),
                subscription.target(),
                subscription.retryPolicy(),
                subscription.createdAt(),
                subscription.updatedAt()
        );
    }
}
