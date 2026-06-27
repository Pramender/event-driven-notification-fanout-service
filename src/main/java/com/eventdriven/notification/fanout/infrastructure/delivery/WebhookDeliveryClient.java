package com.eventdriven.notification.fanout.infrastructure.delivery;

import com.eventdriven.notification.fanout.application.exception.PermanentDeliveryException;
import com.eventdriven.notification.fanout.application.exception.RetryableDeliveryException;
import com.eventdriven.notification.fanout.application.logging.LogActions;
import com.eventdriven.notification.fanout.application.logging.LogStatus;
import com.eventdriven.notification.fanout.application.logging.StructuredLog;
import com.eventdriven.notification.fanout.domain.InboundEvent;
import com.eventdriven.notification.fanout.domain.Subscription;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;

import java.util.UUID;

/**
 * Sends webhook HTTP POST requests to subscriber endpoints.
 */
@Component
public class WebhookDeliveryClient {

    private static final Logger log = LoggerFactory.getLogger(WebhookDeliveryClient.class);

    private final ObjectMapper objectMapper;

    public WebhookDeliveryClient(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public WebhookDeliveryResult deliver(
            Subscription subscription,
            InboundEvent event,
            UUID deliveryId,
            int attemptNumber) {
        var target = subscription.target();
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(target.timeoutMs());
        factory.setReadTimeout(target.timeoutMs());

        RestClient client = RestClient.builder()
                .requestFactory(factory)
                .build();

        String body = buildEnvelope(event, subscription.subscriptionId(), deliveryId, attemptNumber);
        String idempotencyKey = event.eventId() + ":" + subscription.subscriptionId() + ":" + attemptNumber;

        try {
            var response = client.post()
                    .uri(target.url())
                    .contentType(MediaType.APPLICATION_JSON)
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                    .header("X-Event-Id", event.eventId().toString())
                    .header("X-Delivery-Id", deliveryId.toString())
                    .header("X-Idempotency-Key", idempotencyKey)
                    .headers(headers -> target.headers().forEach(headers::add))
                    .body(body)
                    .retrieve()
                    .toBodilessEntity();
            return new WebhookDeliveryResult(response.getStatusCode().value());
        } catch (HttpStatusCodeException ex) {
            int status = ex.getStatusCode().value();
            StructuredLog.at(log)
                    .level(Level.DEBUG)
                    .action(LogActions.WEBHOOK_REQUEST)
                    .status(status >= 500 || status == 429 ? LogStatus.RETRY : LogStatus.FAILED)
                    .field("httpStatus", status)
                    .field("url", target.url())
                    .message("Webhook returned non-success status")
                    .log();
            if (status >= 500 || status == 429) {
                throw new RetryableDeliveryException("HTTP " + status, status, ex);
            }
            if (status >= 400) {
                throw new PermanentDeliveryException("HTTP " + status, status);
            }
            return new WebhookDeliveryResult(status);
        } catch (ResourceAccessException ex) {
            throw new RetryableDeliveryException("Connection error: " + ex.getMessage(), null, ex);
        }
    }

    private String buildEnvelope(
            InboundEvent event,
            UUID subscriptionId,
            UUID deliveryId,
            int attemptNumber) {
        try {
            ObjectNode envelope = objectMapper.createObjectNode();
            envelope.put("event_id", event.eventId().toString());
            envelope.put("type", event.type());
            envelope.put("source", event.source());
            envelope.set("payload", event.payload());
            envelope.put("subscription_id", subscriptionId.toString());
            envelope.put("delivery_id", deliveryId.toString());
            envelope.put("delivery_attempt", attemptNumber);
            envelope.put("idempotency_key",
                    event.eventId() + ":" + subscriptionId + ":" + attemptNumber);
            return objectMapper.writeValueAsString(envelope);
        } catch (Exception ex) {
            throw new IllegalStateException("Unable to build webhook envelope", ex);
        }
    }
}
