package varga.kirka.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.Experiment;
import varga.kirka.model.ExperimentTag;
import varga.kirka.service.ExperimentService;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/2.0/mlflow/experiments")
@RequiredArgsConstructor
public class ExperimentController {

    private final ExperimentService experimentService;

    @lombok.Data
    public static class CreateExperimentRequest {
        @NotBlank
        @Size(max = 256)
        private String name;
        @Size(max = 1024)
        private String artifact_location;
        private List<Tag> tags;

        @lombok.Data
        public static class Tag {
            @NotBlank @Size(max = 250) private String key;
            @Size(max = 5000) private String value;
        }
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    public static class CreateExperimentResponse {
        private String experiment_id;
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    public static class ExperimentResponse {
        private Experiment experiment;
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    public static class ExperimentsResponse {
        private List<Experiment> experiments;
        private String next_page_token;

        public ExperimentsResponse(List<Experiment> experiments) {
            this(experiments, null);
        }
    }

    @lombok.Data
    public static class UpdateExperimentRequest {
        @NotBlank private String experiment_id;
        @NotBlank @Size(max = 256) private String new_name;
    }

    @lombok.Data
    public static class DeleteExperimentRequest {
        @NotBlank private String experiment_id;
    }

    @lombok.Data
    public static class RestoreExperimentRequest {
        @NotBlank private String experiment_id;
    }

    @lombok.Data
    public static class SetExperimentTagRequest {
        @NotBlank private String experiment_id;
        @NotBlank @Size(max = 250) private String key;
        @Size(max = 5000) private String value;
    }

    @PostMapping("/create")
    public CreateExperimentResponse createExperiment(@Valid @RequestBody CreateExperimentRequest request) throws IOException {
        String name = request.getName();
        log.info("REST request to create experiment: {}", name);
        String artifactLocation = request.getArtifact_location();
        java.util.List<ExperimentTag> tags = new java.util.ArrayList<>();
        if (request.getTags() != null) {
            for (CreateExperimentRequest.Tag tag : request.getTags()) {
                tags.add(new ExperimentTag(tag.getKey(), tag.getValue()));
            }
        }
        String id = experimentService.createExperiment(name, artifactLocation, tags);
        return new CreateExperimentResponse(id);
    }

    @GetMapping("/get")
    public ExperimentResponse getExperiment(@RequestParam("experiment_id") String experimentId) throws IOException {
        return new ExperimentResponse(experimentService.getExperiment(experimentId));
    }

    @GetMapping("/get-by-name")
    public ExperimentResponse getExperimentByName(@RequestParam("experiment_name") String experimentName) throws IOException {
        return new ExperimentResponse(experimentService.getExperimentByName(experimentName));
    }

    @PostMapping("/update")
    public Map<String, Object> updateExperiment(@Valid @RequestBody UpdateExperimentRequest request) throws IOException {
        experimentService.updateExperiment(request.getExperiment_id(), request.getNew_name());
        return Map.of();
    }

    @PostMapping("/delete")
    public Map<String, Object> deleteExperiment(@Valid @RequestBody DeleteExperimentRequest request) throws IOException {
        experimentService.deleteExperiment(request.getExperiment_id());
        return Map.of();
    }

    @PostMapping("/restore")
    public Map<String, Object> restoreExperiment(@Valid @RequestBody RestoreExperimentRequest request) throws IOException {
        experimentService.restoreExperiment(request.getExperiment_id());
        return Map.of();
    }

    @PostMapping("/set-experiment-tag")
    public Map<String, Object> setExperimentTag(@Valid @RequestBody SetExperimentTagRequest request) throws IOException {
        experimentService.setExperimentTag(request.getExperiment_id(), request.getKey(), request.getValue());
        return Map.of();
    }

    @PostMapping("/set-tag")
    public Map<String, Object> setTag(@Valid @RequestBody SetExperimentTagRequest request) throws IOException {
        experimentService.setExperimentTag(request.getExperiment_id(), request.getKey(), request.getValue());
        return Map.of();
    }

    @GetMapping("/list")
    public ExperimentsResponse listExperiments(
            @RequestParam(value = "max_results", required = false) Integer maxResults,
            @RequestParam(value = "page_token", required = false) String pageToken) throws IOException {
        if (maxResults == null && pageToken == null) {
            return new ExperimentsResponse(experimentService.listExperiments());
        }
        var page = experimentService.listExperimentsPaged(maxResults, pageToken);
        return new ExperimentsResponse(page.items(), page.nextPageToken());
    }

    @GetMapping("/search")
    public ExperimentsResponse searchExperiments(@RequestParam(value = "view_type", required = false) String viewType,
                                                @RequestParam(value = "max_results", required = false) Integer maxResults,
                                                @RequestParam(value = "filter", required = false) String filter) throws IOException {
        return new ExperimentsResponse(experimentService.searchExperiments(viewType, maxResults, filter));
    }
}
