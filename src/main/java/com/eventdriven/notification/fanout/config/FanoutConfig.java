package com.eventdriven.notification.fanout.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(FanoutProperties.class)
public class FanoutConfig {
}
