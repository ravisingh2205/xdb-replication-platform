package com.xdb.replication.publication;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class PublicationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PublicationServiceApplication.class, args);
    }
}
