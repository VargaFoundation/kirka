package varga.kirka.controller;

import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.Run;
import varga.kirka.service.RunService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/2.0/mlflow/runs")
public class RunController {

    @Autowired
    private RunService runService;

    @PostMapping("/create")
    public Map<String, Object> createRun(@RequestBody Map<String, Object> request) throws IOException {
        String experimentId = (String) request.get("experiment_id");
        log.info("REST request to create run for experiment: {}", experimentId);
        String userId = (String) request.get("user_id");
        long startTime = request.containsKey("start_time") ? ((Number) request.get("start_time")).longValue() : System.currentTimeMillis();
        
        java.util.List<Map<String, String>> tags = (java.util.List<Map<String, String>>) request.get("tags");
        java.util.Map<String, String> tagsMap = new java.util.HashMap<>();
        if (tags != null) {
            for (Map<String, String> tag : tags) {
                tagsMap.put(tag.get("key"), tag.get("value"));
            }
        }

        Run run = runService.createRun(experimentId, userId, startTime, tagsMap);
        return Map.of("run", run);
    }

    @GetMapping("/get")
    public Map<String, Object> getRun(@RequestParam("run_id") String runId) throws IOException {
        return Map.of("run", runService.getRun(runId));
    }

    @PostMapping("/update")
    public Map<String, Object> updateRun(@RequestBody Map<String, Object> request) throws IOException {
        String runId = (String) request.get("run_id");
        String status = (String) request.get("status");
        long endTime = request.containsKey("end_time") ? ((Number) request.get("end_time")).longValue() : 0L;
        runService.updateRun(runId, status, endTime);
        return Map.of();
    }

    @PostMapping("/delete")
    public Map<String, Object> deleteRun(@RequestBody Map<String, String> request) throws IOException {
        runService.deleteRun(request.get("run_id"));
        return Map.of();
    }

    @PostMapping("/restore")
    public Map<String, Object> restoreRun(@RequestBody Map<String, String> request) throws IOException {
        runService.restoreRun(request.get("run_id"));
        return Map.of();
    }

    @PostMapping("/log-batch")
    public Map<String, Object> logBatch(@RequestBody Map<String, Object> request) throws IOException {
        String runId = (String) request.get("run_id");
        java.util.List<Map<String, Object>> metrics = (java.util.List<Map<String, Object>>) request.get("metrics");
        java.util.List<Map<String, String>> params = (java.util.List<Map<String, String>>) request.get("params");
        java.util.List<Map<String, String>> tags = (java.util.List<Map<String, String>>) request.get("tags");
        runService.logBatch(runId, metrics, params, tags);
        return Map.of();
    }

    @PostMapping("/set-tag")
    public Map<String, Object> setTag(@RequestBody Map<String, String> request) throws IOException {
        runService.setTag(request.get("run_id"), request.get("key"), request.get("value"));
        return Map.of();
    }

    @PostMapping("/delete-tag")
    public Map<String, Object> deleteTag(@RequestBody Map<String, String> request) throws IOException {
        runService.deleteTag(request.get("run_id"), request.get("key"));
        return Map.of();
    }

    @PostMapping("/search")
    public Map<String, Object> searchRuns(@RequestBody Map<String, Object> request) throws IOException {
        java.util.List<String> experimentIds = (java.util.List<String>) request.get("experiment_ids");
        String filter = (String) request.get("filter");
        String runViewType = (String) request.get("run_view_type");
        return Map.of("runs", runService.searchRuns(experimentIds, filter, runViewType));
    }

    @GetMapping("/get-metric-history")
    public Map<String, Object> getMetricHistory(@RequestParam("run_id") String runId, @RequestParam("metric_key") String metricKey) throws IOException {
        return Map.of("metrics", runService.getMetricHistory(runId, metricKey));
    }

    @PostMapping("/log-parameter")
    public Map<String, Object> logParameter(@RequestBody Map<String, String> request) throws IOException {
        runService.logParameter(request.get("run_id"), request.get("key"), request.get("value"));
        return Map.of();
    }

    @PostMapping("/log-metric")
    public Map<String, Object> logMetric(@RequestBody Map<String, Object> request) throws IOException {
        String runId = (String) request.get("run_id");
        String key = (String) request.get("key");
        double value = ((Number) request.get("value")).doubleValue();
        long timestamp = request.containsKey("timestamp") ? ((Number) request.get("timestamp")).longValue() : System.currentTimeMillis();
        long step = request.containsKey("step") ? ((Number) request.get("step")).longValue() : 0L;
        
        runService.logMetric(runId, key, value, timestamp, step);
        return Map.of();
    }
}
