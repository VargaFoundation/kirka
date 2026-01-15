package varga.kirka.service;

import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.Run;
import varga.kirka.repo.RunRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.UUID;

@Slf4j
@Service
public class RunService {

    @Autowired
    private RunRepository runRepository;

    public Run createRun(String experimentId, String userId, long startTime, java.util.Map<String, String> tags) throws IOException {
        log.info("Creating run for experimentId: {}, userId: {}", experimentId, userId);
        String runId = UUID.randomUUID().toString();
        Run run = Run.builder()
                .runId(runId)
                .experimentId(experimentId)
                .status("RUNNING")
                .startTime(startTime)
                .artifactUri("hdfs:///mlflow/artifacts/" + experimentId + "/" + runId)
                .tags(tags)
                .build();
        runRepository.createRun(run);
        return run;
    }

    public Run getRun(String runId) throws IOException {
        log.debug("Fetching run: {}", runId);
        return runRepository.getRun(runId);
    }

    public void updateRun(String runId, String status, long endTime) throws IOException {
        log.info("Updating run: {} with status: {} and endTime: {}", runId, status, endTime);
        runRepository.updateRun(runId, status, endTime);
    }

    public void deleteRun(String runId) throws IOException {
        runRepository.deleteRun(runId);
    }

    public void restoreRun(String runId) throws IOException {
        runRepository.restoreRun(runId);
    }

    public void logBatch(String runId, java.util.List<java.util.Map<String, Object>> metrics, 
                         java.util.List<java.util.Map<String, String>> params, 
                         java.util.List<java.util.Map<String, String>> tags) throws IOException {
        runRepository.logBatch(runId, metrics, params, tags);
    }

    public void setTag(String runId, String key, String value) throws IOException {
        runRepository.setTag(runId, key, value);
    }

    public void deleteTag(String runId, String key) throws IOException {
        runRepository.deleteTag(runId, key);
    }

    public java.util.List<Run> searchRuns(java.util.List<String> experimentIds, String filter, String runViewType) throws IOException {
        java.util.List<Run> runs = runRepository.searchRuns(experimentIds, filter, runViewType);
        
        // Filtrage supplémentaire côté service si nécessaire
        if (runViewType != null) {
            runs = runs.stream().filter(r -> {
                if ("active_only".equalsIgnoreCase(runViewType)) {
                    // Pour les runs, active_only est souvent le défaut. 
                    // Supposons que deleteRun marque le status comme 'DELETED' ou utilise lifecycle_stage (non présent dans le modèle Run actuel)
                    return !"DELETED".equalsIgnoreCase(r.getStatus());
                }
                return true;
            }).collect(java.util.stream.Collectors.toList());
        }

        return runs;
    }

    public java.util.List<varga.kirka.model.Metric> getMetricHistory(String runId, String metricKey) throws IOException {
        return runRepository.getMetricHistory(runId, metricKey);
    }

    public void logParameter(String runId, String key, String value) throws IOException {
        runRepository.logBatch(runId, null, java.util.List.of(java.util.Map.of("key", key, "value", value)), null);
    }

    public void logMetric(String runId, String key, double value, long timestamp, long step) throws IOException {
        runRepository.logBatch(runId, java.util.List.of(java.util.Map.of("key", key, "value", value, "timestamp", timestamp, "step", step)), null, null);
    }
}
