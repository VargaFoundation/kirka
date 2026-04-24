package varga.kirka.repair;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import varga.kirka.model.Experiment;
import varga.kirka.repo.ExperimentRepository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Periodically reconciles the {@code mlflow_experiments_name_index} HBase table with the
 * primary {@code mlflow_experiments} table. Because HBase does not offer cross-row or
 * cross-table transactions, a write failure between the two tables can leave an index entry
 * pointing at an experiment that no longer exists (or that has been marked deleted and whose
 * name must become available again).
 *
 * <p>Every hour, this job scans the index and removes entries whose primary row is either
 * missing or in the {@code deleted} lifecycle stage. The number of cleaned rows is exposed
 * on the {@code kirka.repair.orphans} counter for alerting — a sustained non-zero rate is a
 * signal that HBase is experiencing partial failures worth investigating.
 */
@Slf4j
@Component
public class ExperimentIndexReconciler {

    private final ExperimentRepository experimentRepository;
    private final Counter orphansCounter;

    public ExperimentIndexReconciler(ExperimentRepository experimentRepository, MeterRegistry meterRegistry) {
        this.experimentRepository = experimentRepository;
        this.orphansCounter = Counter.builder("kirka.repair.orphans")
                .description("Name-index entries removed by the experiment index reconciler")
                .tag("service", "kirka")
                .register(meterRegistry);
    }

    @Scheduled(fixedDelayString = "${kirka.repair.experiment-index.interval-ms:3600000}",
               initialDelayString = "${kirka.repair.experiment-index.initial-delay-ms:600000}")
    public void reconcile() {
        log.debug("Starting experiment name-index reconciliation");
        List<String[]> orphans = new ArrayList<>();
        try {
            experimentRepository.forEachNameIndexEntry((name, experimentId) -> {
                try {
                    Experiment experiment = experimentRepository.getExperiment(experimentId);
                    boolean isMissing = experiment == null;
                    boolean isDeleted = experiment != null && "deleted".equals(experiment.getLifecycleStage());
                    if (isMissing || isDeleted) {
                        orphans.add(new String[]{name, experimentId, isMissing ? "missing" : "deleted"});
                    }
                } catch (IOException e) {
                    log.warn("Skipped index entry {} -> {} due to lookup error: {}",
                            name, experimentId, e.getMessage());
                }
            });
        } catch (IOException e) {
            log.error("Failed to scan the experiment name index", e);
            return;
        }

        for (String[] orphan : orphans) {
            String name = orphan[0];
            String experimentId = orphan[1];
            String reason = orphan[2];
            try {
                experimentRepository.deleteNameIndexEntry(name);
                orphansCounter.increment();
                log.info("Removed orphan index entry name={} -> id={} reason={}", name, experimentId, reason);
            } catch (IOException e) {
                log.warn("Failed to delete orphan index entry {}: {}", name, e.getMessage());
            }
        }
        if (!orphans.isEmpty()) {
            log.info("Reconciliation complete: {} orphan entries removed", orphans.size());
        }
    }
}
