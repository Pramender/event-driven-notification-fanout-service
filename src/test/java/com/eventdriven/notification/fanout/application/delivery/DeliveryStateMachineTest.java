package com.eventdriven.notification.fanout.application.delivery;

import com.eventdriven.notification.fanout.application.exception.IllegalStateTransitionException;
import com.eventdriven.notification.fanout.domain.DeliveryStatus;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeliveryStateMachineTest {

    @Test
    void allowsValidTransitions() {
        DeliveryStateMachine.assertTransition(DeliveryStatus.QUEUED, DeliveryStatus.IN_FLIGHT);
        DeliveryStateMachine.assertTransition(DeliveryStatus.IN_FLIGHT, DeliveryStatus.SENT);
        DeliveryStateMachine.assertTransition(DeliveryStatus.IN_FLIGHT, DeliveryStatus.RETRY_PENDING);
    }

    @Test
    void rejectsInvalidTransition() {
        assertThatThrownBy(() ->
                DeliveryStateMachine.assertTransition(DeliveryStatus.SENT, DeliveryStatus.IN_FLIGHT))
                .isInstanceOf(IllegalStateTransitionException.class);
    }

    @Test
    void identifiesTerminalStates() {
        assertThat(DeliveryStateMachine.isTerminal(DeliveryStatus.SENT)).isTrue();
        assertThat(DeliveryStateMachine.isTerminal(DeliveryStatus.FAILED)).isTrue();
        assertThat(DeliveryStateMachine.isTerminal(DeliveryStatus.QUEUED)).isFalse();
    }
}
