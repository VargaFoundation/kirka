package varga.kirka.config;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * JSON naming policy. MLFlow clients (Python/Java/R) expect snake_case on the wire, which is
 * also what the reference MLFlow REST API emits. Kirka historically returned camelCase for
 * model fields, so a transitional flag is exposed to let operators (and the legacy test suite)
 * keep the old serialization while we migrate call sites and controller tests.
 *
 * <p>Set {@code kirka.api.naming=snake_case} (the recommended value) to match MLFlow. The default
 * is {@code camel_case} until all existing JSONPath assertions in the controller tests have been
 * converted. Plan to remove this flag once the migration is complete.
 */
@Slf4j
@Configuration
public class JacksonConfig {

    @Bean
    public Jackson2ObjectMapperBuilderCustomizer namingStrategyCustomizer(
            @Value("${kirka.api.naming:camel_case}") String namingStrategy) {
        String strategy = namingStrategy != null ? namingStrategy.trim().toLowerCase() : "camel_case";
        log.info("Kirka JSON naming strategy: {}", strategy);
        return builder -> {
            switch (strategy) {
                case "snake_case" -> builder.propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
                case "camel_case" -> {
                    // Jackson default (no transformation): keeps historical camelCase output
                    // for Experiment/Run/RegisteredModel fields while the test suite migrates.
                }
                default ->
                    log.warn("Unknown kirka.api.naming={}; keeping Jackson default naming", strategy);
            }
        };
    }
}
