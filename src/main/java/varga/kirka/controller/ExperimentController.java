package varga.kirka.controller;

import varga.kirka.model.Experiment;
import varga.kirka.service.ExperimentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/2.0/mlflow/experiments")
public class ExperimentController {

    @Autowired
    private ExperimentService experimentService;

    @PostMapping("/create")
    public Map<String, String> createExperiment(@RequestBody Map<String, String> request) throws IOException {
        String name = request.get("name");
        String artifactLocation = request.get("artifact_location");
        String id = experimentService.createExperiment(name, artifactLocation);
        return Map.of("experiment_id", id);
    }

    @GetMapping("/get")
    public Map<String, Object> getExperiment(@RequestParam("experiment_id") String experimentId) throws IOException {
        return Map.of("experiment", experimentService.getExperiment(experimentId));
    }

    @GetMapping("/get-by-name")
    public Map<String, Object> getExperimentByName(@RequestParam("experiment_name") String experimentName) throws IOException {
        return Map.of("experiment", experimentService.getExperimentByName(experimentName));
    }

    @PostMapping("/update")
    public Map<String, Object> updateExperiment(@RequestBody Map<String, String> request) throws IOException {
        experimentService.updateExperiment(request.get("experiment_id"), request.get("new_name"));
        return Map.of();
    }

    @PostMapping("/delete")
    public Map<String, Object> deleteExperiment(@RequestBody Map<String, String> request) throws IOException {
        experimentService.deleteExperiment(request.get("experiment_id"));
        return Map.of();
    }

    @PostMapping("/restore")
    public Map<String, Object> restoreExperiment(@RequestBody Map<String, String> request) throws IOException {
        experimentService.restoreExperiment(request.get("experiment_id"));
        return Map.of();
    }

    @PostMapping("/set-experiment-tag")
    public Map<String, Object> setExperimentTag(@RequestBody Map<String, String> request) throws IOException {
        experimentService.setExperimentTag(request.get("experiment_id"), request.get("key"), request.get("value"));
        return Map.of();
    }

    @GetMapping("/list")
    public Map<String, List<Experiment>> listExperiments() throws IOException {
        return Map.of("experiments", experimentService.listExperiments());
    }
    @GetMapping("/search")
    public Map<String, Object> searchExperiments(@RequestParam(value = "view_type", required = false) String viewType,
                                                @RequestParam(value = "max_results", required = false) Integer maxResults,
                                                @RequestParam(value = "filter", required = false) String filter) throws IOException {
        return Map.of("experiments", experimentService.searchExperiments(viewType, maxResults, filter));
    }
}
