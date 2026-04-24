package varga.kirka.controller;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.*;
import varga.kirka.service.RunService;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

@Slf4j
@RestController
@RequestMapping("/api/2.0/mlflow/runs")
@RequiredArgsConstructor
public class RunController {

    private final RunService runService;

    @lombok.Data
    public static class CreateRunRequest {
        @NotBlank private String experiment_id;
        @Size(max = 256) private String user_id;
        private Long start_time;
        private List<Tag> tags;

        @lombok.Data
        public static class Tag {
            private String key;
            private String value;
        }
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    public static class RunResponse {
        private Run run;
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    public static class RunsResponse {
        private List<Run> runs;
    }

    @lombok.Data
    public static class UpdateRunRequest {
        @NotBlank private String run_id;
        private String status;
        private Long end_time;
    }

    @lombok.Data
    public static class DeleteRunRequest {
        @NotBlank private String run_id;
    }

    @lombok.Data
    public static class RestoreRunRequest {
        @NotBlank private String run_id;
    }

    @lombok.Data
    public static class LogBatchRequest {
        @NotBlank private String run_id;
        private List<MetricData> metrics;
        private List<ParamData> params;
        private List<TagData> tags;

        @lombok.Data
        public static class MetricData {
            private String key;
            private Double value;
            private Long timestamp;
            private Long step;
        }

        @lombok.Data
        public static class ParamData {
            private String key;
            private String value;
        }

        @lombok.Data
        public static class TagData {
            private String key;
            private String value;
        }
    }

    @lombok.Data
    public static class SetTagRequest {
        @NotBlank private String run_id;
        @NotBlank @Size(max = 250) private String key;
        @Size(max = 5000) private String value;
    }

    @lombok.Data
    public static class DeleteTagRequest {
        @NotBlank private String run_id;
        @NotBlank @Size(max = 250) private String key;
    }

    @lombok.Data
    public static class SearchRunsRequest {
        private List<String> experiment_ids;
        private String filter;
        private String run_view_type;
    }

    @lombok.Data
    public static class LogParameterRequest {
        @NotBlank private String run_id;
        @NotBlank @Size(max = 250) private String key;
        @Size(max = 6000) private String value;
    }

    @lombok.Data
    public static class LogMetricRequest {
        @NotBlank private String run_id;
        @NotBlank @Size(max = 250) private String key;
        private Double value;
        private Long timestamp;
        private Long step;
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    public static class MetricHistoryResponse {
        private List<Metric> metrics;
    }

    @PostMapping("/create")
    public RunResponse createRun(@Valid @RequestBody CreateRunRequest request) throws IOException {
        String experimentId = request.getExperiment_id();
        log.info("REST request to create run for experiment: {}", experimentId);
        String userId = request.getUser_id();
        long startTime = request.getStart_time() != null ? request.getStart_time() : System.currentTimeMillis();
        
        Map<String, String> tags = new HashMap<>();
        if (request.getTags() != null) {
            for (CreateRunRequest.Tag tag : request.getTags()) {
                tags.put(tag.getKey(), tag.getValue());
            }
        }

        Run run = runService.createRun(experimentId, userId, startTime, tags);
        return new RunResponse(run);
    }

    @GetMapping("/get")
    public RunResponse getRun(@RequestParam("run_id") String runId) throws IOException {
        return new RunResponse(runService.getRun(runId));
    }

    @PostMapping("/update")
    public Map<String, Object> updateRun(@Valid @RequestBody UpdateRunRequest request) throws IOException {
        String runId = request.getRun_id();
        String status = request.getStatus();
        long endTime = request.getEnd_time() != null ? request.getEnd_time() : 0L;
        runService.updateRun(runId, status, endTime);
        return Map.of();
    }

    @PostMapping("/delete")
    public Map<String, Object> deleteRun(@Valid @RequestBody DeleteRunRequest request) throws IOException {
        runService.deleteRun(request.getRun_id());
        return Map.of();
    }

    @PostMapping("/restore")
    public Map<String, Object> restoreRun(@Valid @RequestBody RestoreRunRequest request) throws IOException {
        runService.restoreRun(request.getRun_id());
        return Map.of();
    }

    @PostMapping("/log-batch")
    public Map<String, Object> logBatch(@Valid @RequestBody LogBatchRequest request) throws IOException {
        String runId = request.getRun_id();
        
        List<Map<String, Object>> metrics = new ArrayList<>();
        if (request.getMetrics() != null) {
            for (LogBatchRequest.MetricData m : request.getMetrics()) {
                metrics.add(Map.of(
                    "key", m.getKey(),
                    "value", m.getValue(),
                    "timestamp", m.getTimestamp() != null ? m.getTimestamp() : System.currentTimeMillis(),
                    "step", m.getStep() != null ? m.getStep() : 0L
                ));
            }
        }

        List<Map<String, String>> params = new ArrayList<>();
        if (request.getParams() != null) {
            for (LogBatchRequest.ParamData p : request.getParams()) {
                params.add(Map.of("key", p.getKey(), "value", p.getValue()));
            }
        }

        List<Map<String, String>> tags = new ArrayList<>();
        if (request.getTags() != null) {
            for (LogBatchRequest.TagData t : request.getTags()) {
                tags.add(Map.of("key", t.getKey(), "value", t.getValue()));
            }
        }

        runService.logBatch(runId, metrics, params, tags);
        return Map.of();
    }

    @PostMapping("/set-tag")
    public Map<String, Object> setTag(@Valid @RequestBody SetTagRequest request) throws IOException {
        runService.setTag(request.getRun_id(), request.getKey(), request.getValue());
        return Map.of();
    }

    @PostMapping("/delete-tag")
    public Map<String, Object> deleteTag(@Valid @RequestBody DeleteTagRequest request) throws IOException {
        runService.deleteTag(request.getRun_id(), request.getKey());
        return Map.of();
    }

    @PostMapping("/search")
    public RunsResponse searchRuns(@RequestBody SearchRunsRequest request) throws IOException {
        List<String> experimentIds = request.getExperiment_ids();
        String filter = request.getFilter();
        String runViewType = request.getRun_view_type();
        return new RunsResponse(runService.searchRuns(experimentIds, filter, runViewType));
    }

    @GetMapping("/get-metric-history")
    public MetricHistoryResponse getMetricHistory(@RequestParam("run_id") String runId, @RequestParam("metric_key") String metricKey) throws IOException {
        return new MetricHistoryResponse(runService.getMetricHistory(runId, metricKey));
    }

    @PostMapping("/log-parameter")
    public Map<String, Object> logParameter(@Valid @RequestBody LogParameterRequest request) throws IOException {
        runService.logParameter(request.getRun_id(), request.getKey(), request.getValue());
        return Map.of();
    }

    @PostMapping("/log-metric")
    public Map<String, Object> logMetric(@Valid @RequestBody LogMetricRequest request) throws IOException {
        String runId = request.getRun_id();
        List<Map<String, Object>> metrics = List.of(Map.of(
            "key", request.getKey(),
            "value", request.getValue(),
            "timestamp", request.getTimestamp() != null ? request.getTimestamp() : System.currentTimeMillis(),
            "step", request.getStep() != null ? request.getStep() : 0L
        ));
        runService.logBatch(runId, metrics, null, null);
        return Map.of();
    }

    @lombok.Data
    public static class LogInputsRequest {
        private String run_id;
        private List<Map<String, Object>> datasets;
    }

    @PostMapping("/log-inputs")
    public Map<String, Object> logInputs(@RequestBody LogInputsRequest request) throws IOException {
        runService.logInputs(request.getRun_id(), request.getDatasets());
        return Map.of();
    }

    @lombok.Data
    public static class LogModelRequest {
        private String run_id;
        private String model_json;
    }

    @PostMapping("/log-model")
    public Map<String, Object> logModel(@RequestBody LogModelRequest request) throws IOException {
        runService.logModel(request.getRun_id(), request.getModel_json());
        return Map.of();
    }
}
