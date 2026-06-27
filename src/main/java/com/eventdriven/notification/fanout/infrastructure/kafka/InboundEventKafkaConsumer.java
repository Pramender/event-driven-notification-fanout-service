package com.eventdriven.notification.fanout.infrastructure.kafka;

import com.eventdriven.notification.fanout.application.exception.EventValidationException;
import com.eventdriven.notification.fanout.application.ingest.EventIngestService;
import com.eventdriven.notification.fanout.application.logging.LogActions;
import com.eventdriven.notification.fanout.application.logging.LogStatus;
import com.eventdriven.notification.fanout.application.logging.MdcContext;
import com.eventdriven.notification.fanout.application.logging.StructuredLog;
import com.eventdriven.notification.fanout.config.FanoutProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumes inbound events from Kafka. Commits offset only after successful persistence and fanout enqueue.
 */
@Component
public class InboundEventKafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(InboundEventKafkaConsumer.class);

    private final EventIngestService ingestService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String dlqTopic;

    public InboundEventKafkaConsumer(
            EventIngestService ingestService,
            KafkaTemplate<String, String> kafkaTemplate,
            FanoutProperties properties) {
        this.ingestService = ingestService;
        this.kafkaTemplate = kafkaTemplate;
        this.dlqTopic = properties.kafka().dlqTopic();
    }

    @KafkaListener(topics = "${fanout.kafka.inbound-topic}", groupId = "fanout-ingest")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {
            StructuredLog.at(log)
                    .level(Level.DEBUG)
                    .action(LogActions.EVENT_INGEST)
                    .status(LogStatus.SUCCESS)
                    .field("topic", record.topic())
                    .field("partition", record.partition())
                    .field("offset", record.offset())
                    .message("Kafka message received")
                    .log();
            ingestService.acceptEvent(record.value());
            acknowledgment.acknowledge();
        } catch (EventValidationException ex) {
            StructuredLog.at(log)
                    .level(Level.WARN)
                    .action(LogActions.EVENT_INGEST)
                    .status(LogStatus.DLQ)
                    .field("offset", record.offset())
                    .field("reason", ex.getMessage())
                    .message("Invalid event routed to DLQ")
                    .log();
            kafkaTemplate.send(dlqTopic, record.key(), record.value());
            acknowledgment.acknowledge();
        } catch (Exception ex) {
            StructuredLog.at(log)
                    .level(Level.ERROR)
                    .action(LogActions.EVENT_INGEST)
                    .status(LogStatus.FAILED)
                    .field("offset", record.offset())
                    .field("reason", ex.getMessage())
                    .message("Event ingest failed; offset not committed")
                    .error(ex)
                    .log();
            throw ex;
        } finally {
            MdcContext.clear();
        }
    }
}
