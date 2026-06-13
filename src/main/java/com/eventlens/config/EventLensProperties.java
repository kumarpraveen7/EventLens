package com.eventlens.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "eventlens")
public record EventLensProperties(
        long lateEventThresholdMs,
        int maxEventsPerStream
) {
}
