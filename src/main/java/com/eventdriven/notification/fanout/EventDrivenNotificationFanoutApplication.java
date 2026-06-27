package com.eventdriven.notification.fanout;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the event-driven notification fanout service.
 * Accepts events from Kafka, matches subscriptions, and delivers webhooks in FIFO order per subscription.
 */
@SpringBootApplication
@EnableScheduling
public class EventDrivenNotificationFanoutApplication {

    public static void main(String[] args) {
        SpringApplication.run(EventDrivenNotificationFanoutApplication.class, args);
    }
}
