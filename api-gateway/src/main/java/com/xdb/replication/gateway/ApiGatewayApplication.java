package com.xdb.replication.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * API Gateway pattern: the UI and API clients see one origin. Routes are in
 * {@code application.yml}; {@code /internal/**} is deliberately not routed.
 */
@SpringBootApplication
public class ApiGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiGatewayApplication.class, args);
    }
}
