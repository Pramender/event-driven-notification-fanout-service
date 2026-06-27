package com.eventdriven.notification.fanout.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Provisions Kafka topics used by the fanout pipeline.
 */
@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic inboundTopic(FanoutProperties properties) {
        return TopicBuilder.name(properties.kafka().inboundTopic()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic deliveryTopic(FanoutProperties properties) {
        return TopicBuilder.name(properties.kafka().deliveryTopic()).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic dlqTopic(FanoutProperties properties) {
        return TopicBuilder.name(properties.kafka().dlqTopic()).partitions(1).replicas(1).build();
    }
}
