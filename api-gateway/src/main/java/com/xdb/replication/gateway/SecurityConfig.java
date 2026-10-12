package com.xdb.replication.gateway;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity;
import org.springframework.security.config.web.server.ServerHttpSecurity;
import org.springframework.security.web.server.SecurityWebFilterChain;

/**
 * Authentication at the edge. With {@code xdb.security.enabled=true} (the AWS profile) every
 * {@code /api/**} call needs a JWT, e.g. from Amazon Cognito, validated against the issuer's JWKS.
 * Locally it is off so the stack runs without an identity provider.
 */
@Configuration
@EnableWebFluxSecurity
public class SecurityConfig {

    @Bean
    SecurityWebFilterChain security(ServerHttpSecurity http,
                                    @Value("${xdb.security.enabled:false}") boolean enabled) {
        http.csrf(ServerHttpSecurity.CsrfSpec::disable);
        if (!enabled) {
            return http.authorizeExchange(a -> a.anyExchange().permitAll()).build();
        }
        return http
                .authorizeExchange(a -> a
                        .pathMatchers("/actuator/health/**", "/actuator/prometheus").permitAll()
                        .pathMatchers("/api/**").authenticated()
                        .anyExchange().denyAll())
                .oauth2ResourceServer(o -> o.jwt(Customizer.withDefaults()))
                .build();
    }
}
