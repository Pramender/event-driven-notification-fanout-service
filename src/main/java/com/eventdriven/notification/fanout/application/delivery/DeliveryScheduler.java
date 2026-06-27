package com.eventdriven.notification.fanout.application.delivery;

import com.eventdriven.notification.fanout.application.logging.LogActions;
import com.eventdriven.notification.fanout.application.logging.LogStatus;
import com.eventdriven.notification.fanout.application.logging.StructuredLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically polls head-of-line eligible deliveries and dispatches webhook attempts.
 */
@Component
public class DeliveryScheduler {

    private static final Logger log = LoggerFactory.getLogger(DeliveryScheduler.class);

    private final DeliveryOrchestrator orchestrator;

    public DeliveryScheduler(DeliveryOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @Scheduled(fixedDelayString = "${fanout.delivery.worker-poll-interval-ms}")
    public void pollDeliveries() {
        try {
            orchestrator.processReadyDeliveries();
        } catch (Exception ex) {
            StructuredLog.at(log)
                    .level(Level.ERROR)
                    .action(LogActions.DELIVERY_SCHEDULER)
                    .status(LogStatus.FAILED)
                    .field("reason", ex.getMessage())
                    .message("Delivery scheduler iteration failed")
                    .error(ex)
                    .log();
        }
    }
}
