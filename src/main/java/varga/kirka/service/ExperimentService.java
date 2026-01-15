package varga.kirka.service;

import lombok.extern.slf4j.Slf4j;
import varga.kirka.repo.ExperimentRepository;
import varga.kirka.model.Experiment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
public class ExperimentService {

    @Autowired
    private ExperimentRepository experimentRepository;

    public String createExperiment(String name, String artifactLocation, java.util.Map<String, String> tags) throws IOException {
        log.info("Creating experiment with name: {}", name);
        String experimentId = UUID.randomUUID().toString();
        Experiment experiment = Experiment.builder()
                .experimentId(experimentId)
                .name(name)
                .artifactLocation(artifactLocation)
                .lifecycleStage("active")
                .creationTime(System.currentTimeMillis())
                .lastUpdateTime(System.currentTimeMillis())
                .tags(tags)
                .build();
        experimentRepository.createExperiment(experiment);
        return experimentId;
    }

    public Experiment getExperiment(String experimentId) throws IOException {
        log.debug("Fetching experiment: {}", experimentId);
        return experimentRepository.getExperiment(experimentId);
    }

    public Experiment getExperimentByName(String name) throws IOException {
        return experimentRepository.getExperimentByName(name);
    }

    public void updateExperiment(String experimentId, String newName) throws IOException {
        experimentRepository.updateExperiment(experimentId, newName);
    }

    public void deleteExperiment(String experimentId) throws IOException {
        experimentRepository.deleteExperiment(experimentId);
    }

    public void restoreExperiment(String experimentId) throws IOException {
        experimentRepository.restoreExperiment(experimentId);
    }

    public void setExperimentTag(String experimentId, String key, String value) throws IOException {
        experimentRepository.setExperimentTag(experimentId, key, value);
    }

    public List<Experiment> listExperiments() throws IOException {
        return experimentRepository.listExperiments();
    }
    public List<Experiment> searchExperiments(String viewType, Integer maxResults, String filter) throws IOException {
        // Simple implementation: list all
        return experimentRepository.listExperiments();
    }
}
