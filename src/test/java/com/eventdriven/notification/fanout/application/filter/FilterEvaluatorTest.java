package com.eventdriven.notification.fanout.application.filter;

import com.eventdriven.notification.fanout.domain.InboundEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class FilterEvaluatorTest {

    private FilterEvaluator evaluator;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        evaluator = new FilterEvaluator();
        objectMapper = new ObjectMapper();
    }

    @Test
    void matchesTypeAndPayloadConditions() throws Exception {
        ObjectNode filter = objectMapper.readValue("""
                {
                  "all": [
                    { "field": "type", "op": "eq", "value": "order.created" },
                    { "field": "payload.amount", "op": "gte", "value": 100 }
                  ]
                }
                """, ObjectNode.class);

        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("amount", 150);

        InboundEvent event = new InboundEvent(
                UUID.randomUUID(), "order.created", "billing", payload, Instant.now(), Instant.now(), null);

        assertThat(evaluator.matches(filter, event)).isTrue();
    }

    @Test
    void rejectsWhenSourceNotInList() throws Exception {
        ObjectNode filter = objectMapper.readValue("""
                {
                  "all": [
                    { "field": "source", "op": "in", "value": ["billing-service"] }
                  ]
                }
                """, ObjectNode.class);

        InboundEvent event = new InboundEvent(
                UUID.randomUUID(), "order.created", "other", objectMapper.createObjectNode(),
                Instant.now(), Instant.now(), null);

        assertThat(evaluator.matches(filter, event)).isFalse();
    }

    @Test
    void supportsAnyComposition() throws Exception {
        ObjectNode filter = objectMapper.readValue("""
                {
                  "any": [
                    { "field": "type", "op": "eq", "value": "a" },
                    { "field": "type", "op": "eq", "value": "b" }
                  ]
                }
                """, ObjectNode.class);

        InboundEvent event = new InboundEvent(
                UUID.randomUUID(), "b", "src", objectMapper.createObjectNode(),
                Instant.now(), Instant.now(), null);

        assertThat(evaluator.matches(filter, event)).isTrue();
    }
}
