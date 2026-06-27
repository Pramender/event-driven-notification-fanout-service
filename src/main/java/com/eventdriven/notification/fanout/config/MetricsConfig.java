package com.eventdriven.notification.fanout.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

    @Bean
    public Timer deliveryLatencyTimer(MeterRegistry registry) {
        return Timer.builder("delivery.latency")
                .description("Webhook delivery latency")
                .tag("component", "delivery")
                .register(registry);
    }
}
