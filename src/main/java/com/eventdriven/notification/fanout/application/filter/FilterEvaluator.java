package com.eventdriven.notification.fanout.application.filter;

import com.eventdriven.notification.fanout.application.exception.FilterEvaluationException;
import com.eventdriven.notification.fanout.domain.InboundEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.springframework.stereotype.Component;


/**
 * Evaluates version-1 subscription filter DSL against inbound events.
 *
 * <p>Supported structure:
 * <pre>{@code
 * { "version": 1, "all": [ { "field": "type", "op": "eq", "value": "..." }, ... ] }
 * }</pre>
 * Use {@code "any"} instead of {@code "all"} for OR composition. Nested groups supported.
 */
@Component
public class FilterEvaluator {

    public boolean matches(JsonNode filter, InboundEvent event) {
        if (filter == null || filter.isNull()) {
            return true;
        }
        try {
            return evaluateNode(filter, event);
        } catch (Exception ex) {
            throw new FilterEvaluationException("Failed to evaluate filter: " + ex.getMessage(), ex);
        }
    }

    private boolean evaluateNode(JsonNode node, InboundEvent event) {
        if (node.has("all")) {
            for (JsonNode child : node.get("all")) {
                if (!evaluateNode(child, event)) {
                    return false;
                }
            }
            return true;
        }
        if (node.has("any")) {
            for (JsonNode child : node.get("any")) {
                if (evaluateNode(child, event)) {
                    return true;
                }
            }
            return false;
        }
        return evaluateCondition(node, event);
    }

    private boolean evaluateCondition(JsonNode condition, InboundEvent event) {
        String field = requiredText(condition, "field");
        String op = requiredText(condition, "op");
        JsonNode expected = condition.get("value");
        JsonNode actual = resolveField(field, event);

        return switch (op) {
            case "eq" -> jsonEquals(actual, expected);
            case "neq" -> !jsonEquals(actual, expected);
            case "in" -> inList(actual, expected, false);
            case "not_in" -> !inList(actual, expected, false);
            case "exists" -> actual != null && !actual.isNull();
            case "contains" -> actual != null && actual.isTextual()
                    && expected != null && expected.isTextual()
                    && actual.asText().contains(expected.asText());
            case "gte" -> compareNumeric(actual, expected) >= 0;
            case "gt" -> compareNumeric(actual, expected) > 0;
            case "lte" -> compareNumeric(actual, expected) <= 0;
            case "lt" -> compareNumeric(actual, expected) < 0;
            default -> throw new FilterEvaluationException("Unsupported filter op: " + op);
        };
    }

    private JsonNode resolveField(String field, InboundEvent event) {
        return switch (field) {
            case "type" -> textNode(event.type());
            case "source" -> textNode(event.source());
            default -> {
                if (field.startsWith("payload.")) {
                    yield navigatePayload(event.payload(), field.substring("payload.".length()));
                }
                throw new FilterEvaluationException("Unknown field: " + field);
            }
        };
    }

    private JsonNode navigatePayload(JsonNode payload, String path) {
        if (payload == null || payload.isNull()) {
            return null;
        }
        JsonNode current = payload;
        for (String segment : path.split("\\.")) {
            if (current == null || current.isNull()) {
                return null;
            }
            current = current.get(segment);
        }
        return current;
    }

    private JsonNode textNode(String value) {
        return value == null ? null : new TextNode(value);
    }

    private String requiredText(JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null || !value.isTextual()) {
            throw new FilterEvaluationException("Filter condition missing field: " + field);
        }
        return value.asText();
    }

    private boolean jsonEquals(JsonNode actual, JsonNode expected) {
        if (actual == null || actual.isNull()) {
            return expected == null || expected.isNull();
        }
        if (expected == null || expected.isNull()) {
            return false;
        }
        if (actual.isNumber() && expected.isNumber()) {
            return Double.compare(actual.asDouble(), expected.asDouble()) == 0;
        }
        return actual.asText().equals(expected.asText());
    }

    private boolean inList(JsonNode actual, JsonNode expectedArray, boolean unused) {
        if (actual == null || expectedArray == null || !expectedArray.isArray()) {
            return false;
        }
        String actualText = actual.asText();
        for (JsonNode item : expectedArray) {
            if (jsonEquals(actual, item) || actualText.equals(item.asText())) {
                return true;
            }
        }
        return false;
    }

    private int compareNumeric(JsonNode actual, JsonNode expected) {
        if (actual == null || expected == null || !actual.isNumber() || !expected.isNumber()) {
            throw new FilterEvaluationException("Numeric comparison requires numeric values");
        }
        return Double.compare(actual.asDouble(), expected.asDouble());
    }
}
