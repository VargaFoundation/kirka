package varga.kirka.service;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.*;
import varga.kirka.repo.RunRepository;
import varga.kirka.security.SecurityContextHelper;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.UUID;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class RunService {

    private static final String RESOURCE_TYPE = "run";

    private final RunRepository runRepository;

    private final SecurityContextHelper securityContextHelper;

    public Run createRun(String experimentId, String userId, long startTime, Map<String, String> tagsMap) throws IOException {
        log.info("Creating run for experimentId: {}, userId: {}", experimentId, userId);
        if (experimentId == null || experimentId.isBlank()) {
            throw new IllegalArgumentException("experiment_id must not be empty");
        }
        String runId = UUID.randomUUID().toString();
        // Use current user as owner if userId is not provided
        String owner = (userId != null && !userId.isEmpty()) ? userId : securityContextHelper.getCurrentUser();
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
                        .userId(owner)
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
        Run run = runRepository.getRun(runId);
        if (run == null) {
            throw new ResourceNotFoundException("Run", runId);
        }
        Map<String, String> tagsMap = getRunTagsMap(run);
        securityContextHelper.checkReadAccess(RESOURCE_TYPE, runId, run.getInfo().getUserId(), tagsMap);
        return run;
    }

    public void updateRun(String runId, String status, long endTime) throws IOException {
        log.info("Updating run: {} with status: {} and endTime: {}", runId, status, endTime);
        Run run = runRepository.getRun(runId);
        if (run == null) {
            throw new ResourceNotFoundException("Run", runId);
        }
        Map<String, String> tagsMap = getRunTagsMap(run);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, runId, run.getInfo().getUserId(), tagsMap);
        runRepository.updateRun(runId, status, endTime);
    }

    public void deleteRun(String runId) throws IOException {
        Run run = runRepository.getRun(runId);
        if (run == null) {
            throw new ResourceNotFoundException("Run", runId);
        }
        Map<String, String> tagsMap = getRunTagsMap(run);
        securityContextHelper.checkDeleteAccess(RESOURCE_TYPE, runId, run.getInfo().getUserId(), tagsMap);
        runRepository.deleteRun(runId);
    }

    public void restoreRun(String runId) throws IOException {
        Run run = runRepository.getRun(runId);
        if (run == null) {
            throw new ResourceNotFoundException("Run", runId);
        }
        Map<String, String> tagsMap = getRunTagsMap(run);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, runId, run.getInfo().getUserId(), tagsMap);
        runRepository.restoreRun(runId);
    }

    public void logBatch(String runId, List<Map<String, Object>> metricsData,
                         List<Map<String, String>> paramsData,
                         List<Map<String, String>> tagsData) throws IOException {
        Run run = runRepository.getRun(runId);
        if (run == null) {
            throw new ResourceNotFoundException("Run", runId);
        }
        Map<String, String> tagsMap = getRunTagsMap(run);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, runId, run.getInfo().getUserId(), tagsMap);

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
        Run run = runRepository.getRun(runId);
        if (run == null) {
            throw new ResourceNotFoundException("Run", runId);
        }
        Map<String, String> tagsMap = getRunTagsMap(run);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, runId, run.getInfo().getUserId(), tagsMap);
        runRepository.setTag(runId, key, value);
    }

    public void deleteTag(String runId, String key) throws IOException {
        Run run = runRepository.getRun(runId);
        if (run == null) {
            throw new ResourceNotFoundException("Run", runId);
        }
        Map<String, String> tagsMap = getRunTagsMap(run);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, runId, run.getInfo().getUserId(), tagsMap);
        runRepository.deleteTag(runId, key);
    }

    public List<Run> searchRuns(List<String> experimentIds, String filter, String runViewType) throws IOException {
        List<Run> runs = runRepository.searchRuns(experimentIds, filter, runViewType);

        // Filter by read access
        runs = runs.stream()
                .filter(run -> {
                    Map<String, String> tagsMap = getRunTagsMap(run);
                    return securityContextHelper.canRead(RESOURCE_TYPE, run.getInfo().getRunId(),
                            run.getInfo().getUserId(), tagsMap);
                })
                .collect(Collectors.toList());

        if (runViewType != null) {
            runs = runs.stream().filter(r -> {
                if ("ACTIVE_ONLY".equalsIgnoreCase(runViewType)) {
                    return "active".equalsIgnoreCase(r.getInfo().getLifecycleStage());
                } else if ("DELETED_ONLY".equalsIgnoreCase(runViewType)) {
                    return "deleted".equalsIgnoreCase(r.getInfo().getLifecycleStage());
                }
                return true;
            }).collect(Collectors.toList());
        }

        return runs;
    }

    public List<Metric> getMetricHistory(String runId, String metricKey) throws IOException {
        Run run = runRepository.getRun(runId);
        if (run == null) {
            throw new ResourceNotFoundException("Run", runId);
        }
        Map<String, String> tagsMap = getRunTagsMap(run);
        securityContextHelper.checkReadAccess(RESOURCE_TYPE, runId, run.getInfo().getUserId(), tagsMap);
        return runRepository.getMetricHistory(runId, metricKey);
    }

    public void logParameter(String runId, String key, String value) throws IOException {
        Run run = runRepository.getRun(runId);
        if (run == null) {
            throw new ResourceNotFoundException("Run", runId);
        }
        Map<String, String> tagsMap = getRunTagsMap(run);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, runId, run.getInfo().getUserId(), tagsMap);
        runRepository.logBatch(runId, null, List.of(new Param(key, value)), null);
    }

    public void logMetric(String runId, String key, double value, long timestamp, long step) throws IOException {
        Run run = runRepository.getRun(runId);
        if (run == null) {
            throw new ResourceNotFoundException("Run", runId);
        }
        Map<String, String> tagsMap = getRunTagsMap(run);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, runId, run.getInfo().getUserId(), tagsMap);
        runRepository.logBatch(runId, List.of(new Metric(key, value, timestamp, step)), null, null);
    }

    /**
     * Extracts tags from a Run as a Map for authorization checks.
     */
    private Map<String, String> getRunTagsMap(Run run) {
        if (run.getData() == null || run.getData().getTags() == null) {
            return Map.of();
        }
        return securityContextHelper.tagsToMap(run.getData().getTags(), RunTag::getKey, RunTag::getValue);
    }
}
