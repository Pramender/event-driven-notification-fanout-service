package com.eventdriven.notification.fanout.application.filter;

import com.eventdriven.notification.fanout.application.exception.FilterEvaluationException;
import com.eventdriven.notification.fanout.domain.InboundEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void nullFilterMatchesAll() {
        InboundEvent event = new InboundEvent(
                UUID.randomUUID(), "any", "src", objectMapper.createObjectNode(),
                Instant.now(), Instant.now(), null);
        assertThat(evaluator.matches(null, event)).isTrue();
    }

    @Test
    void rejectsUnsupportedOperator() throws Exception {
        ObjectNode filter = objectMapper.readValue("""
                { "all": [ { "field": "type", "op": "regex", "value": ".*" } ] }
                """, ObjectNode.class);
        InboundEvent event = new InboundEvent(
                UUID.randomUUID(), "order.created", "src", objectMapper.createObjectNode(),
                Instant.now(), Instant.now(), null);

        assertThatThrownBy(() -> evaluator.matches(filter, event))
                .isInstanceOf(FilterEvaluationException.class)
                .hasMessageContaining("Unsupported filter op");
    }

    @Test
    void rejectsUnknownField() throws Exception {
        ObjectNode filter = objectMapper.readValue("""
                { "all": [ { "field": "unknown", "op": "eq", "value": "x" } ] }
                """, ObjectNode.class);
        InboundEvent event = new InboundEvent(
                UUID.randomUUID(), "order.created", "src", objectMapper.createObjectNode(),
                Instant.now(), Instant.now(), null);

        assertThatThrownBy(() -> evaluator.matches(filter, event))
                .isInstanceOf(FilterEvaluationException.class)
                .hasMessageContaining("Unknown field");
    }

    @Test
    void rejectsNumericComparisonOnNonNumericValues() throws Exception {
        ObjectNode filter = objectMapper.readValue("""
                { "all": [ { "field": "payload.amount", "op": "gte", "value": 100 } ] }
                """, ObjectNode.class);
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("amount", "not-a-number");
        InboundEvent event = new InboundEvent(
                UUID.randomUUID(), "order.created", "src", payload,
                Instant.now(), Instant.now(), null);

        assertThatThrownBy(() -> evaluator.matches(filter, event))
                .isInstanceOf(FilterEvaluationException.class)
                .hasMessageContaining("Numeric comparison");
    }

    @Test
    void rejectsMissingConditionField() throws Exception {
        ObjectNode filter = objectMapper.readValue("""
                { "all": [ { "op": "eq", "value": "x" } ] }
                """, ObjectNode.class);
        InboundEvent event = new InboundEvent(
                UUID.randomUUID(), "order.created", "src", objectMapper.createObjectNode(),
                Instant.now(), Instant.now(), null);

        assertThatThrownBy(() -> evaluator.matches(filter, event))
                .isInstanceOf(FilterEvaluationException.class)
                .hasMessageContaining("missing field");
    }
}
