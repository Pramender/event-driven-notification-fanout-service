package com.eventdriven.notification.fanout.application.logging;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.slf4j.event.Level;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class StructuredLogTest {

    private ListAppender<ILoggingEvent> appender;
    private Logger logger;

    @BeforeEach
    void setUp() {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        appender = new ListAppender<>();
        appender.setContext(context);
        appender.start();

        ch.qos.logback.classic.Logger logbackLogger =
                context.getLogger(StructuredLogTest.class);
        logbackLogger.addAppender(appender);
        logbackLogger.setLevel(ch.qos.logback.classic.Level.DEBUG);

        logger = logbackLogger;
    }

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void emitsActionStatusAndFieldsWithoutLeakingMdc() {
        UUID deliveryId = UUID.randomUUID();
        MdcContext.putDelivery(deliveryId, null, null);

        StructuredLog.at(logger)
                .level(Level.ERROR)
                .action(LogActions.DELIVERY_DISPATCH)
                .status(LogStatus.FAILED)
                .field("attempt", 2)
                .field("reason", "HTTP 503")
                .message("Delivery permanently failed")
                .log();

        ILoggingEvent event = appender.list.getLast();
        assertThat(event.getLevel().toString()).isEqualTo("ERROR");
        assertThat(event.getFormattedMessage()).isEqualTo("Delivery permanently failed");
        assertThat(event.getMDCPropertyMap())
                .containsEntry("action", LogActions.DELIVERY_DISPATCH)
                .containsEntry("status", LogStatus.FAILED.name())
                .containsEntry("attempt", "2")
                .containsEntry("reason", "HTTP 503")
                .containsEntry("details", "attempt=2 reason=HTTP 503")
                .containsEntry("deliveryId", deliveryId.toString());

        assertThat(MDC.getCopyOfContextMap())
                .containsEntry("deliveryId", deliveryId.toString())
                .doesNotContainKey("action")
                .doesNotContainKey("status")
                .doesNotContainKey("details");
    }
}
