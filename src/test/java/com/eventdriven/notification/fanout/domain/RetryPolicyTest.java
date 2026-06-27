package com.eventdriven.notification.fanout.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryPolicyTest {

    @Test
    void computesBackoffWithinBounds() {
        RetryPolicy policy = new RetryPolicy(5, 1000, 5000, 2.0);
        long backoff = policy.computeBackoffMs(3);
        assertThat(backoff).isBetween(0L, 5000L);
    }

    @Test
    void appliesDefaultsForInvalidValues() {
        RetryPolicy policy = new RetryPolicy(3, -1, 100, 0.5);
        assertThat(policy.maxAttempts()).isEqualTo(3);
        assertThat(policy.initialBackoffMs()).isEqualTo(1000);
        assertThat(policy.multiplier()).isEqualTo(2.0);
    }

    @Test
    void rejectsNonPositiveMaxAttempts() {
        assertThatThrownBy(() -> new RetryPolicy(0, 1000, 5000, 2.0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxAttempts");
    }
}
