package com.eventdriven.notification.fanout;

import com.eventdriven.notification.fanout.application.audit.AuditService;
import com.eventdriven.notification.fanout.application.audit.DeliveryAuditView;
import com.eventdriven.notification.fanout.application.delivery.DeliveryOrchestrator;
import com.eventdriven.notification.fanout.application.ingest.EventIngestService;
import com.eventdriven.notification.fanout.application.replay.ReplayResult;
import com.eventdriven.notification.fanout.application.replay.ReplayService;
import com.eventdriven.notification.fanout.application.subscription.SubscriptionService;
import com.eventdriven.notification.fanout.domain.DeliveryMode;
import com.eventdriven.notification.fanout.domain.DeliveryStatus;
import com.eventdriven.notification.fanout.domain.RetryPolicy;
import com.eventdriven.notification.fanout.domain.WebhookTarget;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
class FanoutIntegrationIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("fanout")
            .withUsername("fanout")
            .withPassword("fanout");

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    static WireMockServer webhookServer;

    @Autowired
    EventIngestService ingestService;

    @Autowired
    SubscriptionService subscriptionService;

    @Autowired
    DeliveryOrchestrator deliveryOrchestrator;

    @Autowired
    AuditService auditService;

    @Autowired
    ReplayService replayService;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeAll
    static void startWebhook() {
        webhookServer = new WireMockServer(0);
        webhookServer.start();
        WireMock.configureFor("localhost", webhookServer.port());
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
    void endToEndEventFanoutDeliveryAndAudit() throws Exception {
        webhookServer.resetAll();
        stubFor(post(urlEqualTo("/hook"))
                .willReturn(aResponse().withStatus(200)));

        var filter = objectMapper.readTree("""
                { "all": [ { "field": "type", "op": "eq", "value": "order.created" } ] }
                """);
        var subscription = subscriptionService.create(
                "orders-hook",
                DeliveryMode.AT_LEAST_ONCE,
                filter,
                new WebhookTarget("http://localhost:" + webhookServer.port() + "/hook", Map.of(), 3000),
                new RetryPolicy(3, 100, 500, 2.0)
        );

        String eventJson = """
                {
                  "event_id": "%s",
                  "type": "order.created",
                  "source": "orders-api",
                  "payload": { "order_id": "42" }
                }
                """.formatted(UUID.randomUUID());

        var event = ingestService.acceptEvent(eventJson);
        deliveryOrchestrator.processReadyDeliveries();

        await().untilAsserted(() -> verify(postRequestedFor(urlEqualTo("/hook"))));

        List<DeliveryAuditView> audit = auditService.byEvent(event.eventId());
        assertThat(audit).hasSize(1);
        assertThat(audit.getFirst().finalStatus()).isEqualTo(DeliveryStatus.SENT);
        assertThat(audit.getFirst().attempts()).isNotEmpty();
        assertThat(audit.getFirst().attempts().getFirst().httpStatus()).isEqualTo(200);
    }

    @Test
    void fifoOrderMaintainedPerSubscription() throws Exception {
        webhookServer.resetAll();
        stubFor(post(urlEqualTo("/fifo"))
                .inScenario("fifo")
                .whenScenarioStateIs("Started")
                .willReturn(aResponse().withStatus(503))
                .willSetStateTo("FirstFailed"));
        stubFor(post(urlEqualTo("/fifo"))
                .inScenario("fifo")
                .whenScenarioStateIs("FirstFailed")
                .willReturn(aResponse().withStatus(200)));

        var filter = objectMapper.readTree("""
                { "all": [ { "field": "source", "op": "eq", "value": "fifo-test" } ] }
                """);
        subscriptionService.create(
                "fifo-sub",
                DeliveryMode.AT_LEAST_ONCE,
                filter,
                new WebhookTarget("http://localhost:" + webhookServer.port() + "/fifo", Map.of(), 3000),
                new RetryPolicy(5, 10, 100, 2.0)
        );

        ingestService.acceptEvent("""
                {"type":"t","source":"fifo-test","payload":{"n":1}}
                """);
        ingestService.acceptEvent("""
                {"type":"t","source":"fifo-test","payload":{"n":2}}
                """);

        deliveryOrchestrator.processReadyDeliveries();
        await().untilAsserted(() -> verify(1, postRequestedFor(urlEqualTo("/fifo"))));

        deliveryOrchestrator.processReadyDeliveries();
        await().untilAsserted(() -> verify(2, postRequestedFor(urlEqualTo("/fifo"))));

        deliveryOrchestrator.processReadyDeliveries();
        await().untilAsserted(() -> verify(3, postRequestedFor(urlEqualTo("/fifo"))));
    }

    @Test
    void noMatchingSubscriptionProducesNoDeliveries() throws Exception {
        webhookServer.resetAll();
        stubFor(post(urlMatching("/hook.*"))
                .willReturn(aResponse().withStatus(200)));

        var filter = objectMapper.readTree("""
                { "all": [ { "field": "type", "op": "eq", "value": "never.match" } ] }
                """);
        subscriptionService.create(
                "no-match",
                DeliveryMode.AT_LEAST_ONCE,
                filter,
                new WebhookTarget("http://localhost:" + webhookServer.port() + "/hook-nomatch", Map.of(), 3000),
                new RetryPolicy(3, 100, 500, 2.0)
        );

        var event = ingestService.acceptEvent("""
                { "type": "other.type", "source": "test", "payload": {} }
                """);

        deliveryOrchestrator.processReadyDeliveries();
        verify(0, postRequestedFor(urlEqualTo("/hook-nomatch")));
        assertThat(auditService.byEvent(event.eventId())).isEmpty();
    }

    @Test
    void retryableWebhookFailureEventuallySucceeds() throws Exception {
        webhookServer.resetAll();
        stubFor(post(urlEqualTo("/retry-hook"))
                .inScenario("retry")
                .whenScenarioStateIs("Started")
                .willReturn(aResponse().withStatus(503))
                .willSetStateTo("FailedOnce"));
        stubFor(post(urlEqualTo("/retry-hook"))
                .inScenario("retry")
                .whenScenarioStateIs("FailedOnce")
                .willReturn(aResponse().withStatus(200)));

        var filter = objectMapper.readTree("""
                { "all": [ { "field": "type", "op": "eq", "value": "retry.test" } ] }
                """);
        subscriptionService.create(
                "retry-sub",
                DeliveryMode.AT_LEAST_ONCE,
                filter,
                new WebhookTarget("http://localhost:" + webhookServer.port() + "/retry-hook", Map.of(), 3000),
                new RetryPolicy(5, 10, 100, 2.0)
        );

        var event = ingestService.acceptEvent("""
                { "type": "retry.test", "source": "test", "payload": {} }
                """);

        deliveryOrchestrator.processReadyDeliveries();
        await().untilAsserted(() -> verify(1, postRequestedFor(urlEqualTo("/retry-hook"))));

        await().untilAsserted(() -> {
            deliveryOrchestrator.processReadyDeliveries();
            List<DeliveryAuditView> audit = auditService.byEvent(event.eventId());
            assertThat(audit).hasSize(1);
            assertThat(audit.getFirst().finalStatus()).isEqualTo(DeliveryStatus.SENT);
            assertThat(audit.getFirst().attempts()).hasSizeGreaterThanOrEqualTo(2);
        });
    }

    @Test
    void replayRequeuesHistoricalEventsInTimeRange() throws Exception {
        webhookServer.resetAll();
        stubFor(post(urlEqualTo("/replay-hook"))
                .willReturn(aResponse().withStatus(200)));

        var filter = objectMapper.readTree("""
                { "all": [ { "field": "type", "op": "eq", "value": "replay.service.event" } ] }
                """);
        var subscription = subscriptionService.create(
                "replay-sub",
                DeliveryMode.AT_LEAST_ONCE,
                filter,
                new WebhookTarget("http://localhost:" + webhookServer.port() + "/replay-hook", Map.of(), 3000),
                new RetryPolicy(3, 100, 500, 2.0)
        );

        Instant windowStart = Instant.parse("2026-06-27T10:00:00Z");
        Instant windowEnd = Instant.parse("2026-06-27T12:00:00Z");

        ingestService.acceptEvent("""
                {
                  "type": "replay.service.event",
                  "source": "replay-test",
                  "occurred_at": "2026-06-27T10:30:00Z",
                  "payload": { "order_id": "1" }
                }
                """);
        ingestService.acceptEvent("""
                {
                  "type": "payment.completed",
                  "source": "replay-test",
                  "occurred_at": "2026-06-27T11:30:00Z",
                  "payload": {}
                }
                """);

        deliveryOrchestrator.processReadyDeliveries();
        await().untilAsserted(() -> verify(1, postRequestedFor(urlEqualTo("/replay-hook"))));

        ReplayResult result = replayService.replay(subscription.subscriptionId(), windowStart, windowEnd);
        assertThat(result.eventsScanned()).isEqualTo(2);
        assertThat(result.matched()).isEqualTo(1);
        assertThat(result.queued()).isEqualTo(1);
        assertThat(result.skipped()).isZero();

        await().untilAsserted(() -> {
            deliveryOrchestrator.processReadyDeliveries();
            verify(2, postRequestedFor(urlEqualTo("/replay-hook")));
        });
    }
}
