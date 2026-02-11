package varga.kirka.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import varga.kirka.model.Scorer;
import varga.kirka.service.ScorerService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/2.0/mlflow/scorers")
@RequiredArgsConstructor
public class ScorerController {

    private final ScorerService scorerService;

    @lombok.Data
    @lombok.AllArgsConstructor
    public static class ScorersResponse {
        private List<Scorer> scorers;
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    public static class ScorerResponse {
        private Scorer scorer;
    }

    @lombok.Data
    public static class RegisterScorerRequest {
        private String experiment_id;
        private String name;
        private String serialized_scorer;
    }

    @GetMapping("/list")
    public ScorersResponse listScorers(@RequestParam("experiment_id") String experimentId) throws IOException {
        log.info("REST request to list scorers for experiment: {}", experimentId);
        List<Scorer> scorers = scorerService.listScorers(experimentId);
        return new ScorersResponse(scorers);
    }

    @GetMapping("/versions")
    public ScorersResponse listScorerVersions(
            @RequestParam("experiment_id") String experimentId,
            @RequestParam("name") String name) throws IOException {
        log.info("REST request to list scorer versions for experiment: {}, name: {}", experimentId, name);
        List<Scorer> scorers = scorerService.listScorerVersions(experimentId, name);
        return new ScorersResponse(scorers);
    }

    @PostMapping("/register")
    public ScorerResponse registerScorer(@RequestBody RegisterScorerRequest request) throws IOException {
        String experimentId = request.getExperiment_id();
        String name = request.getName();
        String serializedScorer = request.getSerialized_scorer();
        log.info("REST request to register scorer: {} for experiment: {}", name, experimentId);
        Scorer scorer = scorerService.registerScorer(experimentId, name, serializedScorer);
        return new ScorerResponse(scorer);
    }

    @GetMapping("/get")
    public ScorerResponse getScorer(
            @RequestParam("experiment_id") String experimentId,
            @RequestParam("name") String name,
            @RequestParam(value = "version", required = false) Integer version) throws IOException {
        log.info("REST request to get scorer: {}, version: {} for experiment: {}", name, version, experimentId);
        Scorer scorer = scorerService.getScorer(experimentId, name, version);
        return new ScorerResponse(scorer);
    }

    @DeleteMapping("/delete")
    public Map<String, Object> deleteScorer(
            @RequestParam("experiment_id") String experimentId,
            @RequestParam("name") String name,
            @RequestParam(value = "version", required = false) Integer version) throws IOException {
        log.info("REST request to delete scorer: {}, version: {} for experiment: {}", name, version, experimentId);
        scorerService.deleteScorer(experimentId, name, version);
        return Map.of();
    }
}
