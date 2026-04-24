package varga.kirka.observability;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * Records a latency sample on the {@code kirka.hbase.operations} timer for every call into
 * {@code varga.kirka.repo.*}, tagged by {@code repository}, {@code operation} and
 * {@code outcome} (success/failure). Operators can then slice by any dimension in Grafana:
 *
 * <pre>
 *   kirka_hbase_operations_seconds_count{repository="ExperimentRepository",operation="createExperiment",outcome="success"}
 * </pre>
 *
 * <p>The aspect deliberately wraps repository classes — not services — because services chain
 * several repository calls and individual repository latencies tell a more actionable story
 * when investigating HBase slowdowns.
 */
@Slf4j
@Aspect
@Component
public class RepositoryTimingAspect {

    private final MeterRegistry registry;

    public RepositoryTimingAspect(MeterRegistry registry) {
        this.registry = registry;
    }

    @Around("execution(public * varga.kirka.repo..*(..))")
    public Object recordLatency(ProceedingJoinPoint pjp) throws Throwable {
        long startNanos = System.nanoTime();
        String outcome = "success";
        try {
            return pjp.proceed();
        } catch (Throwable t) {
            outcome = "failure";
            throw t;
        } finally {
            long elapsed = System.nanoTime() - startNanos;
            String repository = pjp.getSignature().getDeclaringType().getSimpleName();
            String operation = pjp.getSignature().getName();
            Timer.builder("kirka.hbase.operations")
                    .description("Latency of HBase-backed repository operations")
                    .tag("service", "kirka")
                    .tag("repository", repository)
                    .tag("operation", operation)
                    .tag("outcome", outcome)
                    .register(registry)
                    .record(elapsed, TimeUnit.NANOSECONDS);
            if (log.isTraceEnabled()) {
                log.trace("{}::{} outcome={} elapsed_ms={}",
                        repository, operation, outcome, elapsed / 1_000_000);
            }
        }
    }
}
