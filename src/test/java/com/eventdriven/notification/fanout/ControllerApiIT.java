package com.eventdriven.notification.fanout;

import com.eventdriven.notification.fanout.application.delivery.DeliveryOrchestrator;
import com.eventdriven.notification.fanout.domain.DeliveryStatus;
import com.eventdriven.notification.fanout.infrastructure.web.dto.AcceptEventResponse;
import com.eventdriven.notification.fanout.infrastructure.web.dto.DeliveryAuditResponse;
import com.eventdriven.notification.fanout.infrastructure.web.dto.ReplayEventsResponse;
import com.eventdriven.notification.fanout.infrastructure.web.dto.SubscriptionResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.*;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Exercises every REST controller endpoint over HTTP with three sample webhook consumers.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class ControllerApiIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("fanout")
            .withUsername("fanout")
            .withPassword("fanout");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    static WireMockServer webhookServer;

    @LocalServerPort
    int port;

    @Autowired
    TestRestTemplate restTemplate;

    @Autowired
    DeliveryOrchestrator deliveryOrchestrator;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeAll
    static void startWebhook() {
        webhookServer = new WireMockServer(0);
        webhookServer.start();
        WireMock.configureFor("localhost", webhookServer.port());
        stubFor(post(urlMatching("/consumer-[1-3]"))
                .willReturn(aResponse().withStatus(200)));
    }

    @AfterAll
    static void stopWebhook() {
        if (webhookServer != null) {
            webhookServer.stop();
        }
    }

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("management.tracing.enabled", () -> "false");
        registry.add("spring.kafka.listener.auto-startup", () -> "false");
        registry.add("fanout.delivery.worker-poll-interval-ms", () -> "60000");
        registry.add("fanout.delivery.scheduler-interval-ms", () -> "60000");
    }

    @Test
    void allControllerEndpointsWithThreeConsumersAndEvents() throws Exception {
        webhookServer.resetRequests();
        stubFor(post(urlMatching("/consumer-[1-3]"))
                .willReturn(aResponse().withStatus(200)));

        SubscriptionResponse consumer1 = createSubscription("subscriptions/consumer-1-order-alerts.json");
        SubscriptionResponse consumer2 = createSubscription("subscriptions/consumer-2-payment-hooks.json");
        SubscriptionResponse consumer3 = createSubscription("subscriptions/consumer-3-inventory-alerts.json");

        assertThat(consumer1.name()).isEqualTo("order-alerts");
        assertThat(consumer1.enabled()).isTrue();
        assertThat(consumer1.subscriptionId()).isNotNull();

        SubscriptionResponse fetched = getSubscription(consumer1.subscriptionId());
        assertThat(fetched.name()).isEqualTo("order-alerts");
        assertThat(fetched.deliveryMode()).isEqualTo(consumer1.deliveryMode());

        List<SubscriptionResponse> active = listSubscriptions();
        assertThat(active)
                .extracting(SubscriptionResponse::subscriptionId)
                .contains(consumer1.subscriptionId(), consumer2.subscriptionId(), consumer3.subscriptionId());

        AcceptEventResponse orderHigh = postEvent("events/order-created-high-value.json");
        AcceptEventResponse orderLow = postEvent("events/order-created-low-value.json");
        AcceptEventResponse payment = postEvent("events/payment-completed.json");
        AcceptEventResponse inventory = postEvent("events/inventory-low-stock.json");

        assertThat(orderHigh.eventId()).isEqualTo(UUID.fromString("11111111-1111-1111-1111-111111111111"));
        assertThat(orderHigh.type()).isEqualTo("order.created");
        assertThat(orderHigh.receivedAt()).isNotNull();

        ResponseEntity<String> duplicate = postEventRaw("events/order-created-high-value.json");
        assertThat(duplicate.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        ResponseEntity<String> invalid = postEventRaw("events/invalid-missing-payload.json");
        assertThat(invalid.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(invalid.getBody()).contains("payload");

        dispatchUntilWebhooksReceived(3);

        verify(postRequestedFor(urlEqualTo("/consumer-1")));
        verify(postRequestedFor(urlEqualTo("/consumer-2")));
        verify(postRequestedFor(urlEqualTo("/consumer-3")));
        verify(0, postRequestedFor(urlEqualTo("/consumer-1"))
                .withRequestBody(containing("ORD-1002")));

        List<DeliveryAuditResponse> orderAudits = auditByEvent(orderHigh.eventId());
        assertThat(orderAudits).hasSize(1);
        assertThat(orderAudits.getFirst().subscriptionId()).isEqualTo(consumer1.subscriptionId());
        assertThat(orderAudits.getFirst().finalStatus()).isEqualTo(DeliveryStatus.SENT);
        assertThat(orderAudits.getFirst().attempts()).isNotEmpty();
        assertThat(orderAudits.getFirst().attempts().getFirst().httpStatus()).isEqualTo(200);

        UUID deliveryId = orderAudits.getFirst().deliveryId();
        DeliveryAuditResponse deliveryAudit = auditByDelivery(deliveryId);
        assertThat(deliveryAudit.eventId()).isEqualTo(orderHigh.eventId());
        assertThat(deliveryAudit.sequenceNumber()).isEqualTo(1);

        List<DeliveryAuditResponse> sentForConsumer2 = auditBySubscription(consumer2.subscriptionId(), "SENT");
        assertThat(sentForConsumer2).hasSize(1);
        assertThat(sentForConsumer2.getFirst().eventId()).isEqualTo(payment.eventId());

        List<DeliveryAuditResponse> allForConsumer3 = auditBySubscription(consumer3.subscriptionId(), null);
        assertThat(allForConsumer3).hasSize(1);
        assertThat(allForConsumer3.getFirst().eventId()).isEqualTo(inventory.eventId());

        List<DeliveryAuditResponse> noMatchForConsumer1 = auditBySubscription(consumer1.subscriptionId(), "FAILED");
        assertThat(noMatchForConsumer1).isEmpty();

        List<DeliveryAuditResponse> lowValueAudits = auditByEvent(orderLow.eventId());
        assertThat(lowValueAudits).isEmpty();

        deleteSubscription(consumer3.subscriptionId());
        assertThat(getSubscriptionStatus(consumer3.subscriptionId())).isEqualTo(HttpStatus.NOT_FOUND);

        List<SubscriptionResponse> afterDelete = listSubscriptions();
        assertThat(afterDelete)
                .extracting(SubscriptionResponse::subscriptionId)
                .contains(consumer1.subscriptionId(), consumer2.subscriptionId())
                .doesNotContain(consumer3.subscriptionId());
    }

    @Test
    void subscriptionNotFoundReturns404() {
        UUID missing = UUID.randomUUID();
        assertThat(getSubscriptionStatus(missing)).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void errorResponsesReturnProblemDetails() throws Exception {
        ResponseEntity<String> malformed = restTemplate.exchange(
                apiUrl("/v1/events"),
                HttpMethod.POST,
                jsonEntity("{not-json"),
                String.class);
        assertThat(malformed.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(malformed.getBody()).contains("Invalid request body");

        ResponseEntity<String> missingType = restTemplate.exchange(
                apiUrl("/v1/events"),
                HttpMethod.POST,
                jsonEntity("""
                        { "source": "orders-api", "payload": {} }
                        """),
                String.class);
        assertThat(missingType.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(missingType.getBody()).contains("Validation failed");
        assertThat(missingType.getBody()).contains("type");

        ResponseEntity<String> invalidEventId = restTemplate.exchange(
                apiUrl("/v1/events"),
                HttpMethod.POST,
                jsonEntity("""
                        {
                          "event_id": "not-a-uuid",
                          "type": "order.created",
                          "source": "orders-api",
                          "payload": {}
                        }
                        """),
                String.class);
        assertThat(invalidEventId.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(invalidEventId.getBody()).contains("Invalid request body");

        ResponseEntity<String> missingName = restTemplate.exchange(
                apiUrl("/v1/subscriptions"),
                HttpMethod.POST,
                jsonEntity("""
                        {
                          "deliveryMode": "AT_LEAST_ONCE",
                          "filter": { "all": [] },
                          "target": { "url": "http://localhost:1/hook", "timeoutMs": 1000 },
                          "retryPolicy": { "maxAttempts": 3, "initialBackoffMs": 100, "maxBackoffMs": 500, "multiplier": 2.0 }
                        }
                        """),
                String.class);
        assertThat(missingName.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(missingName.getBody()).contains("name");

        ResponseEntity<String> blankWebhookUrl = restTemplate.exchange(
                apiUrl("/v1/subscriptions"),
                HttpMethod.POST,
                jsonEntity("""
                        {
                          "name": "bad-target",
                          "deliveryMode": "AT_LEAST_ONCE",
                          "filter": { "all": [] },
                          "target": { "url": "  ", "timeoutMs": 1000 },
                          "retryPolicy": { "maxAttempts": 3, "initialBackoffMs": 100, "maxBackoffMs": 500, "multiplier": 2.0 }
                        }
                        """),
                String.class);
        assertThat(blankWebhookUrl.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(blankWebhookUrl.getBody()).contains("Webhook URL is required");

        ResponseEntity<String> invalidRetryPolicy = restTemplate.exchange(
                apiUrl("/v1/subscriptions"),
                HttpMethod.POST,
                jsonEntity("""
                        {
                          "name": "bad-retry",
                          "deliveryMode": "AT_LEAST_ONCE",
                          "filter": { "all": [] },
                          "target": { "url": "http://localhost:1/hook", "timeoutMs": 1000 },
                          "retryPolicy": { "maxAttempts": 0, "initialBackoffMs": 100, "maxBackoffMs": 500, "multiplier": 2.0 }
                        }
                        """),
                String.class);
        assertThat(invalidRetryPolicy.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(invalidRetryPolicy.getBody()).contains("maxAttempts");

        UUID missingSubscription = UUID.randomUUID();
        ResponseEntity<String> deleteMissing = restTemplate.exchange(
                apiUrl("/v1/subscriptions/" + missingSubscription),
                HttpMethod.DELETE,
                HttpEntity.EMPTY,
                String.class);
        assertThat(deleteMissing.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(deleteMissing.getBody()).contains("Subscription not found");

        UUID missingDelivery = UUID.randomUUID();
        ResponseEntity<String> auditMissingDelivery = restTemplate.getForEntity(
                apiUrl("/v1/audit/deliveries/" + missingDelivery),
                String.class);
        assertThat(auditMissingDelivery.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(auditMissingDelivery.getBody()).contains("Delivery not found");

        SubscriptionResponse subscription = createSubscription("subscriptions/consumer-1-order-alerts.json");
        ResponseEntity<String> invalidStatus = restTemplate.getForEntity(
                apiUrl("/v1/audit/subscriptions/" + subscription.subscriptionId() + "?status=NOT_A_STATUS"),
                String.class);
        assertThat(invalidStatus.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(invalidStatus.getBody()).contains("status");
        deleteSubscription(subscription.subscriptionId());
    }

    @Test
    void invalidSubscriptionFilterFailsEventAcceptance() throws Exception {
        String body = """
                {
                  "name": "bad-filter",
                  "deliveryMode": "AT_LEAST_ONCE",
                  "filter": { "all": [ { "field": "type", "op": "regex", "value": ".*" } ] },
                  "target": { "url": "%s/hook", "timeoutMs": 3000 },
                  "retryPolicy": { "maxAttempts": 3, "initialBackoffMs": 100, "maxBackoffMs": 500, "multiplier": 2.0 }
                }
                """.formatted("http://localhost:" + webhookServer.port());
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<String> createResponse = restTemplate.exchange(
                apiUrl("/v1/subscriptions"),
                HttpMethod.POST,
                new HttpEntity<>(body, headers),
                String.class);
        assertThat(createResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        ResponseEntity<String> eventResponse = restTemplate.exchange(
                apiUrl("/v1/events"),
                HttpMethod.POST,
                jsonEntity("""
                        {
                          "type": "order.created",
                          "source": "orders-api",
                          "payload": { "amount": 200 }
                        }
                        """),
                String.class);
        assertThat(eventResponse.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(eventResponse.getBody()).contains("Invalid subscription filter");
        assertThat(eventResponse.getBody()).contains("Unsupported filter op");
    }

    @Test
    void permanentWebhookFailureSurfacesInAudit() throws Exception {
        webhookServer.resetAll();
        stubFor(post(urlEqualTo("/fail-400"))
                .willReturn(aResponse().withStatus(400)));

        String body = """
                {
                  "name": "fail-hook",
                  "deliveryMode": "AT_LEAST_ONCE",
                  "filter": { "all": [ { "field": "type", "op": "eq", "value": "fail.test" } ] },
                  "target": { "url": "%s/fail-400", "timeoutMs": 3000 },
                  "retryPolicy": { "maxAttempts": 3, "initialBackoffMs": 10, "maxBackoffMs": 50, "multiplier": 2.0 }
                }
                """.formatted("http://localhost:" + webhookServer.port());
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<SubscriptionResponse> createResponse = restTemplate.exchange(
                apiUrl("/v1/subscriptions"),
                HttpMethod.POST,
                new HttpEntity<>(body, headers),
                SubscriptionResponse.class);
        assertThat(createResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        AcceptEventResponse event = postEventRawSuccess("""
                {
                  "type": "fail.test",
                  "source": "test",
                  "payload": {}
                }
                """);

        await().untilAsserted(() -> {
            deliveryOrchestrator.processReadyDeliveries();
            List<DeliveryAuditResponse> audits = auditByEvent(event.eventId());
            assertThat(audits).hasSize(1);
            assertThat(audits.getFirst().finalStatus()).isEqualTo(DeliveryStatus.FAILED);
            assertThat(audits.getFirst().attempts()).isNotEmpty();
            assertThat(audits.getFirst().attempts().getFirst().httpStatus()).isEqualTo(400);
        });

        List<DeliveryAuditResponse> failed = auditBySubscription(
                createResponse.getBody().subscriptionId(), "FAILED");
        assertThat(failed).hasSize(1);
        assertThat(failed.getFirst().eventId()).isEqualTo(event.eventId());
    }

    @Test
    void replayApiAcceptsTimeRangeAndRedeliversEvents() throws Exception {
        webhookServer.resetAll();
        stubFor(post(urlEqualTo("/replay-api"))
                .willReturn(aResponse().withStatus(200)));

        String subscriptionBody = """
                {
                  "name": "replay-api-sub",
                  "deliveryMode": "AT_LEAST_ONCE",
                  "filter": { "all": [ { "field": "type", "op": "eq", "value": "replay.api.event" } ] },
                  "target": { "url": "%s/replay-api", "timeoutMs": 3000 },
                  "retryPolicy": { "maxAttempts": 3, "initialBackoffMs": 100, "maxBackoffMs": 500, "multiplier": 2.0 }
                }
                """.formatted("http://localhost:" + webhookServer.port());
        SubscriptionResponse subscription = restTemplate.exchange(
                apiUrl("/v1/subscriptions"),
                HttpMethod.POST,
                jsonEntity(subscriptionBody),
                SubscriptionResponse.class).getBody();

        postEventRawSuccess("""
                {
                  "type": "replay.api.event",
                  "source": "orders-api",
                  "occurred_at": "2026-06-27T10:30:00Z",
                  "payload": { "amount": 150 }
                }
                """);

        await().untilAsserted(() -> {
            deliveryOrchestrator.processReadyDeliveries();
            verify(1, postRequestedFor(urlEqualTo("/replay-api")));
        });

        ResponseEntity<ReplayEventsResponse> replayResponse = restTemplate.exchange(
                apiUrl("/v1/subscriptions/" + subscription.subscriptionId() + "/replay"),
                HttpMethod.POST,
                jsonEntity("""
                        {
                          "from": "2026-06-27T10:00:00Z",
                          "to": "2026-06-27T12:00:00Z"
                        }
                        """),
                ReplayEventsResponse.class);
        assertThat(replayResponse.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(replayResponse.getBody().subscriptionId()).isEqualTo(subscription.subscriptionId());
        assertThat(replayResponse.getBody().matched()).isEqualTo(1);
        assertThat(replayResponse.getBody().queued()).isEqualTo(1);

        await().untilAsserted(() -> {
            deliveryOrchestrator.processReadyDeliveries();
            verify(2, postRequestedFor(urlEqualTo("/replay-api")));
        });

        deleteSubscription(subscription.subscriptionId());
    }

    @Test
    void replayApiReturnsProblemDetailsForInvalidRequests() {
        UUID missing = UUID.randomUUID();
        ResponseEntity<String> notFound = restTemplate.exchange(
                apiUrl("/v1/subscriptions/" + missing + "/replay"),
                HttpMethod.POST,
                jsonEntity("""
                        {
                          "from": "2026-06-27T10:00:00Z",
                          "to": "2026-06-27T12:00:00Z"
                        }
                        """),
                String.class);
        assertThat(notFound.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(notFound.getBody()).contains("Subscription not found");

        SubscriptionResponse subscription = restTemplate.exchange(
                apiUrl("/v1/subscriptions"),
                HttpMethod.POST,
                jsonEntity("""
                        {
                          "name": "replay-errors",
                          "deliveryMode": "AT_LEAST_ONCE",
                          "filter": { "all": [] },
                          "target": { "url": "http://localhost:1/hook", "timeoutMs": 1000 },
                          "retryPolicy": { "maxAttempts": 3, "initialBackoffMs": 100, "maxBackoffMs": 500, "multiplier": 2.0 }
                        }
                        """),
                SubscriptionResponse.class).getBody();

        ResponseEntity<String> invalidRange = restTemplate.exchange(
                apiUrl("/v1/subscriptions/" + subscription.subscriptionId() + "/replay"),
                HttpMethod.POST,
                jsonEntity("""
                        {
                          "from": "2026-06-27T12:00:00Z",
                          "to": "2026-06-27T10:00:00Z"
                        }
                        """),
                String.class);
        assertThat(invalidRange.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(invalidRange.getBody()).contains("Invalid replay request");

        ResponseEntity<String> missingFields = restTemplate.exchange(
                apiUrl("/v1/subscriptions/" + subscription.subscriptionId() + "/replay"),
                HttpMethod.POST,
                jsonEntity("{}"),
                String.class);
        assertThat(missingFields.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(missingFields.getBody()).contains("Validation failed");
    }

    private SubscriptionResponse createSubscription(String samplePath) throws IOException {
        String body = loadSample(samplePath);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<SubscriptionResponse> response = restTemplate.exchange(
                apiUrl("/v1/subscriptions"),
                HttpMethod.POST,
                new HttpEntity<>(body, headers),
                SubscriptionResponse.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return response.getBody();
    }

    private SubscriptionResponse getSubscription(UUID subscriptionId) {
        ResponseEntity<SubscriptionResponse> response = restTemplate.getForEntity(
                apiUrl("/v1/subscriptions/" + subscriptionId),
                SubscriptionResponse.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return response.getBody();
    }

    private HttpStatusCode getSubscriptionStatus(UUID subscriptionId) {
        ResponseEntity<String> response = restTemplate.getForEntity(
                apiUrl("/v1/subscriptions/" + subscriptionId),
                String.class);
        return response.getStatusCode();
    }

    private List<SubscriptionResponse> listSubscriptions() {
        ResponseEntity<SubscriptionResponse[]> response = restTemplate.getForEntity(
                apiUrl("/v1/subscriptions"),
                SubscriptionResponse[].class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return Arrays.asList(response.getBody());
    }

    private void deleteSubscription(UUID subscriptionId) {
        ResponseEntity<Void> response = restTemplate.exchange(
                apiUrl("/v1/subscriptions/" + subscriptionId),
                HttpMethod.DELETE,
                HttpEntity.EMPTY,
                Void.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
    }

    private AcceptEventResponse postEvent(String samplePath) throws IOException {
        ResponseEntity<AcceptEventResponse> response = restTemplate.exchange(
                apiUrl("/v1/events"),
                HttpMethod.POST,
                jsonEntity(loadSample(samplePath)),
                AcceptEventResponse.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        return response.getBody();
    }

    private ResponseEntity<String> postEventRaw(String samplePath) throws IOException {
        return restTemplate.exchange(
                apiUrl("/v1/events"),
                HttpMethod.POST,
                jsonEntity(loadSample(samplePath)),
                String.class);
    }

    private AcceptEventResponse postEventRawSuccess(String body) {
        ResponseEntity<AcceptEventResponse> response = restTemplate.exchange(
                apiUrl("/v1/events"),
                HttpMethod.POST,
                jsonEntity(body),
                AcceptEventResponse.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        return response.getBody();
    }

    private List<DeliveryAuditResponse> auditByEvent(UUID eventId) {
        ResponseEntity<DeliveryAuditResponse[]> response = restTemplate.getForEntity(
                apiUrl("/v1/audit/events/" + eventId),
                DeliveryAuditResponse[].class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return Arrays.asList(response.getBody());
    }

    private List<DeliveryAuditResponse> auditBySubscription(UUID subscriptionId, String status) {
        String path = status == null
                ? "/v1/audit/subscriptions/" + subscriptionId
                : "/v1/audit/subscriptions/" + subscriptionId + "?status=" + status;
        ResponseEntity<DeliveryAuditResponse[]> response = restTemplate.getForEntity(
                apiUrl(path),
                DeliveryAuditResponse[].class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return Arrays.asList(response.getBody());
    }

    private DeliveryAuditResponse auditByDelivery(UUID deliveryId) {
        ResponseEntity<DeliveryAuditResponse> response = restTemplate.getForEntity(
                apiUrl("/v1/audit/deliveries/" + deliveryId),
                DeliveryAuditResponse.class);
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        return response.getBody();
    }

    private void dispatchUntilWebhooksReceived(int expectedCount) {
        await().untilAsserted(() -> {
            deliveryOrchestrator.processReadyDeliveries();
            verify(expectedCount, postRequestedFor(urlMatching("/consumer-[1-3]")));
        });
    }

    private HttpEntity<String> jsonEntity(String body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return new HttpEntity<>(body, headers);
    }

    private String loadSample(String relativePath) throws IOException {
        String raw = new ClassPathResource("api-samples/" + relativePath)
                .getContentAsString(StandardCharsets.UTF_8);
        return raw.replace("{{WEBHOOK_URL}}", "http://localhost:" + webhookServer.port());
    }

    private String apiUrl(String path) {
        return "http://localhost:" + port + path;
    }
}
