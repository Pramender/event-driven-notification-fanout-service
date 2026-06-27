package com.eventdriven.notification.fanout.infrastructure.kafka;

import com.eventdriven.notification.fanout.application.exception.EventValidationException;
import com.eventdriven.notification.fanout.application.ingest.EventIngestService;
import com.eventdriven.notification.fanout.config.FanoutProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
        log.debug("Received Kafka message topic={} partition={} offset={}",
                record.topic(), record.partition(), record.offset());
        try {
            ingestService.acceptEvent(record.value());
            acknowledgment.acknowledge();
        } catch (EventValidationException ex) {
            log.warn("Sending invalid event to DLQ offset={} reason={}", record.offset(), ex.getMessage());
            kafkaTemplate.send(dlqTopic, record.key(), record.value());
            acknowledgment.acknowledge();
        } catch (Exception ex) {
            log.error("Failed to ingest event offset={}; offset will not be committed", record.offset(), ex);
            throw ex;
        }
    }
}
