package varga.kirka.service;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.*;
import varga.kirka.repo.RunRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.UUID;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class RunService {

    @Autowired
    private RunRepository runRepository;

    public Run createRun(String experimentId, String userId, long startTime, Map<String, String> tagsMap) throws IOException {
        log.info("Creating run for experimentId: {}, userId: {}", experimentId, userId);
        String runId = UUID.randomUUID().toString();
        List<RunTag> tags = new java.util.ArrayList<>();
        if (tagsMap != null) {
            for (Map.Entry<String, String> entry : tagsMap.entrySet()) {
                tags.add(RunTag.builder().key(entry.getKey()).value(entry.getValue()).build());
            }
        }
        Run run = Run.builder()
                .info(RunInfo.builder()
                        .runId(runId)
                        .runUuid(runId)
                        .experimentId(experimentId)
                        .userId(userId)
                        .status(RunStatus.RUNNING)
                        .startTime(startTime)
                        .artifactUri("hdfs:///mlflow/artifacts/" + experimentId + "/" + runId)
                        .lifecycleStage("active")
                        .build())
                .data(RunData.builder()
                        .tags(tags)
                        .metrics(List.of())
                        .params(List.of())
                        .build())
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

    public void logBatch(String runId, List<Map<String, Object>> metricsData, 
                         List<Map<String, String>> paramsData, 
                         List<Map<String, String>> tagsData) throws IOException {
        List<Metric> metrics = metricsData != null ? metricsData.stream().map(m -> Metric.builder()
                .key((String) m.get("key"))
                .value(((Number) m.get("value")).doubleValue())
                .timestamp(m.containsKey("timestamp") ? ((Number) m.get("timestamp")).longValue() : System.currentTimeMillis())
                .step(m.containsKey("step") ? ((Number) m.get("step")).longValue() : 0L)
                .build()).collect(Collectors.toList()) : null;
        
        List<Param> params = paramsData != null ? paramsData.stream()
                .map(p -> Param.builder().key(p.get("key")).value(p.get("value")).build())
                .collect(Collectors.toList()) : null;
                
        List<RunTag> tags = tagsData != null ? tagsData.stream()
                .map(t -> RunTag.builder().key(t.get("key")).value(t.get("value")).build())
                .collect(Collectors.toList()) : null;

        runRepository.logBatch(runId, metrics, params, tags);
    }

    public void setTag(String runId, String key, String value) throws IOException {
        runRepository.setTag(runId, key, value);
    }

    public void deleteTag(String runId, String key) throws IOException {
        runRepository.deleteTag(runId, key);
    }

    public List<Run> searchRuns(List<String> experimentIds, String filter, String runViewType) throws IOException {
        List<Run> runs = runRepository.searchRuns(experimentIds, filter, runViewType);
        
        if (runViewType != null) {
            runs = runs.stream().filter(r -> {
                if ("ACTIVE_ONLY".equalsIgnoreCase(runViewType)) {
                    return !"KILLED".equalsIgnoreCase(r.getInfo().getStatus().name());
                }
                return true;
            }).collect(Collectors.toList());
        }

        return runs;
    }

    public List<Metric> getMetricHistory(String runId, String metricKey) throws IOException {
        return runRepository.getMetricHistory(runId, metricKey);
    }

    public void logParameter(String runId, String key, String value) throws IOException {
        runRepository.logBatch(runId, null, List.of(new Param(key, value)), null);
    }

    public void logMetric(String runId, String key, double value, long timestamp, long step) throws IOException {
        runRepository.logBatch(runId, List.of(new Metric(key, value, timestamp, step)), null, null);
    }
}
