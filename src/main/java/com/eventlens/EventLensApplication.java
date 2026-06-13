package com.eventlens;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class EventLensApplication {
    public static void main(String[] args) {
        SpringApplication.run(EventLensApplication.class, args);
    }
}


