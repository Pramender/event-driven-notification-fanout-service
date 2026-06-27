package com.eventdriven.notification.fanout.application.delivery;

import com.eventdriven.notification.fanout.application.exception.IllegalStateTransitionException;
import com.eventdriven.notification.fanout.domain.DeliveryStatus;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Enforces valid delivery status transitions.
 */
public final class DeliveryStateMachine {

    private static final Map<DeliveryStatus, Set<DeliveryStatus>> ALLOWED = Map.of(
            DeliveryStatus.QUEUED, EnumSet.of(DeliveryStatus.IN_FLIGHT),
            DeliveryStatus.IN_FLIGHT, EnumSet.of(DeliveryStatus.SENT, DeliveryStatus.RETRY_PENDING, DeliveryStatus.FAILED),
            DeliveryStatus.RETRY_PENDING, EnumSet.of(DeliveryStatus.IN_FLIGHT),
            DeliveryStatus.SENT, EnumSet.noneOf(DeliveryStatus.class),
            DeliveryStatus.FAILED, EnumSet.noneOf(DeliveryStatus.class)
    );

    private DeliveryStateMachine() {
    }

    public static void assertTransition(DeliveryStatus from, DeliveryStatus to) {
        Set<DeliveryStatus> allowed = ALLOWED.getOrDefault(from, Set.of());
        if (!allowed.contains(to)) {
            throw new IllegalStateTransitionException(
                    "Invalid delivery transition from " + from + " to " + to);
        }
    }

    public static boolean isTerminal(DeliveryStatus status) {
        return status == DeliveryStatus.SENT || status == DeliveryStatus.FAILED;
    }
}
