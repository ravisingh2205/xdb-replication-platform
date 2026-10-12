package com.xdb.replication.apply;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Subscriber / apply service. Owns no metadata database: its configuration arrives as events
 * on the compacted config topics, and its progress lives in Kafka offsets plus a checkpoint
 * table on each target. That makes the pods stateless and horizontally scalable.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class ApplyServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApplyServiceApplication.class, args);
    }
}
