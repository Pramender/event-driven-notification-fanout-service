package com.eventdriven.notification.fanout.domain;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WebhookTargetTest {

    @Test
    void acceptsValidTarget() {
        WebhookTarget target = new WebhookTarget("https://example.com/hook", Map.of("X-Key", "v"), 3000);
        assertThat(target.url()).isEqualTo("https://example.com/hook");
        assertThat(target.headers()).containsEntry("X-Key", "v");
        assertThat(target.timeoutMs()).isEqualTo(3000);
    }

    @Test
    void defaultsTimeoutWhenNonPositive() {
        WebhookTarget target = new WebhookTarget("https://example.com/hook", null, 0);
        assertThat(target.timeoutMs()).isEqualTo(5000);
        assertThat(target.headers()).isEmpty();
    }

    @Test
    void rejectsBlankUrl() {
        assertThatThrownBy(() -> new WebhookTarget("  ", Map.of(), 1000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Webhook URL is required");
    }
}
