package varga.kirka.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Micrometer metrics configuration for Kirka.
 * Exposes custom metrics for monitoring via Prometheus/Actuator.
 */
@Configuration
public class MetricsConfig {

    private final AtomicInteger activeExperiments = new AtomicInteger(0);
    private final AtomicInteger activeRuns = new AtomicInteger(0);
    private final AtomicInteger registeredModels = new AtomicInteger(0);

    @Bean
    public Counter experimentsCreatedCounter(MeterRegistry registry) {
        return Counter.builder("kirka.experiments.created")
                .description("Total number of experiments created")
                .tag("service", "kirka")
                .register(registry);
    }

    @Bean
    public Counter runsCreatedCounter(MeterRegistry registry) {
        return Counter.builder("kirka.runs.created")
                .description("Total number of runs created")
                .tag("service", "kirka")
                .register(registry);
    }

    @Bean
    public Counter runsCompletedCounter(MeterRegistry registry) {
        return Counter.builder("kirka.runs.completed")
                .description("Total number of runs completed")
                .tag("service", "kirka")
                .register(registry);
    }

    @Bean
    public Counter modelsRegisteredCounter(MeterRegistry registry) {
        return Counter.builder("kirka.models.registered")
                .description("Total number of models registered")
                .tag("service", "kirka")
                .register(registry);
    }

    @Bean
    public Counter artifactsUploadedCounter(MeterRegistry registry) {
        return Counter.builder("kirka.artifacts.uploaded")
                .description("Total number of artifacts uploaded")
                .tag("service", "kirka")
                .register(registry);
    }

    @Bean
    public Counter artifactsDownloadedCounter(MeterRegistry registry) {
        return Counter.builder("kirka.artifacts.downloaded")
                .description("Total number of artifacts downloaded")
                .tag("service", "kirka")
                .register(registry);
    }

    @Bean
    public Counter authorizationDeniedCounter(MeterRegistry registry) {
        return Counter.builder("kirka.authorization.denied")
                .description("Number of requests denied due to authorization")
                .tag("service", "kirka")
                .register(registry);
    }

    @Bean
    public Counter authorizationAllowedCounter(MeterRegistry registry) {
        return Counter.builder("kirka.authorization.allowed")
                .description("Number of authorized requests")
                .tag("service", "kirka")
                .register(registry);
    }

    @Bean
    public Timer experimentOperationsTimer(MeterRegistry registry) {
        return Timer.builder("kirka.experiments.operations")
                .description("Time spent on experiment operations")
                .tag("service", "kirka")
                .register(registry);
    }

    @Bean
    public Timer runOperationsTimer(MeterRegistry registry) {
        return Timer.builder("kirka.runs.operations")
                .description("Time spent on run operations")
                .tag("service", "kirka")
                .register(registry);
    }

    @Bean
    public Timer hbaseOperationsTimer(MeterRegistry registry) {
        return Timer.builder("kirka.hbase.operations")
                .description("Time spent on HBase operations")
                .tag("service", "kirka")
                .register(registry);
    }

    @Bean
    public Timer hdfsOperationsTimer(MeterRegistry registry) {
        return Timer.builder("kirka.hdfs.operations")
                .description("Time spent on HDFS operations")
                .tag("service", "kirka")
                .register(registry);
    }

    @Bean
    public AtomicInteger activeExperimentsGauge(MeterRegistry registry) {
        Gauge.builder("kirka.experiments.active", activeExperiments, AtomicInteger::get)
                .description("Number of active experiments")
                .tag("service", "kirka")
                .register(registry);
        return activeExperiments;
    }

    @Bean
    public AtomicInteger activeRunsGauge(MeterRegistry registry) {
        Gauge.builder("kirka.runs.active", activeRuns, AtomicInteger::get)
                .description("Number of active runs")
                .tag("service", "kirka")
                .register(registry);
        return activeRuns;
    }

    @Bean
    public AtomicInteger registeredModelsGauge(MeterRegistry registry) {
        Gauge.builder("kirka.models.total", registeredModels, AtomicInteger::get)
                .description("Total number of registered models")
                .tag("service", "kirka")
                .register(registry);
        return registeredModels;
    }
}
