package varga.kirka.security;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

/**
 * Spring Security configuration.
 * Enabled only if security.enabled=true in application.properties.
 */
@Slf4j
@Configuration
@EnableWebSecurity
@EnableMethodSecurity(prePostEnabled = true)
@ConditionalOnProperty(name = "security.enabled", havingValue = "true")
public class SecurityConfig {

    @Value("${security.authentication.type:basic}")
    private String authenticationType;

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http, 
            KirkaAuthenticationFilter kirkaAuthenticationFilter) throws Exception {
        log.info("Configuring security with authentication type: {}", authenticationType);
        
        http
            .csrf(csrf -> csrf.disable())
            .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(auth -> auth
                // Publicly reachable probes/metrics (no secret material leaked by these endpoints)
                .requestMatchers("/actuator/health", "/actuator/health/**",
                                 "/actuator/info", "/actuator/prometheus",
                                 "/ping", "/version").permitAll()
                // OpenAPI spec + Swagger UI are API metadata, safe to expose
                .requestMatchers("/v3/api-docs", "/v3/api-docs/**",
                                 "/swagger-ui.html", "/swagger-ui/**").permitAll()
                // env/loggers/beans and any other actuator endpoint: admin only
                .requestMatchers("/actuator/**").hasRole("ADMIN")
                // All other endpoints require authentication
                .anyRequest().authenticated()
            )
            .addFilterBefore(kirkaAuthenticationFilter, UsernamePasswordAuthenticationFilter.class);
        
        return http.build();
    }
}
