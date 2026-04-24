package varga.kirka.config;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.stereotype.Component;

/**
 * JSON naming policy. MLFlow clients (Python/Java/R) expect snake_case on the wire, which is
 * what the reference MLFlow REST API emits — that is the production default here. The
 * {@code camel_case} value is kept as a short-term escape hatch for legacy consumers that
 * were coded against Kirka's pre-stabilization output; remove the flag once those callers
 * are gone.
 *
 * <p>This is a {@code @Component} (not a {@code @Configuration} with a {@code @Bean} method) so
 * that Spring Boot's web test slice ({@code @WebMvcTest}) picks it up automatically — the slice
 * does not import arbitrary configuration classes, but it does scan components on the classpath.
 */
@Slf4j
@Component
public class JacksonConfig implements Jackson2ObjectMapperBuilderCustomizer {

    private final String namingStrategy;

    public JacksonConfig(@Value("${kirka.api.naming:snake_case}") String namingStrategy) {
        this.namingStrategy = namingStrategy != null ? namingStrategy.trim().toLowerCase() : "snake_case";
        log.info("Kirka JSON naming strategy: {}", this.namingStrategy);
    }

    @Override
    public void customize(Jackson2ObjectMapperBuilder builder) {
        switch (namingStrategy) {
            case "snake_case" -> builder.propertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
            case "camel_case" -> {
                // Jackson default (no transformation): keeps historical camelCase output for
                // legacy callers. Intentionally a no-op branch.
            }
            default ->
                log.warn("Unknown kirka.api.naming={}; keeping Jackson default naming", namingStrategy);
        }
    }
}
