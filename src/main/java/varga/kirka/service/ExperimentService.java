package varga.kirka.service;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.*;
import varga.kirka.repo.ExperimentRepository;
import varga.kirka.security.SecurityContextHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ExperimentService {

    private static final String RESOURCE_TYPE = "experiment";

    @Autowired
    private ExperimentRepository experimentRepository;

    @Autowired
    private SecurityContextHelper securityContextHelper;

    public String createExperiment(String name, String artifactLocation, List<ExperimentTag> tags) throws IOException {
        log.info("Creating experiment with name: {}", name);
        String experimentId = UUID.randomUUID().toString();
        String currentUser = securityContextHelper.getCurrentUser();
        Experiment experiment = Experiment.builder()
                .experimentId(experimentId)
                .name(name)
                .artifactLocation(artifactLocation)
                .lifecycleStage("active")
                .creationTime(System.currentTimeMillis())
                .lastUpdateTime(System.currentTimeMillis())
                .tags(tags)
                .owner(currentUser)
                .build();
        experimentRepository.createExperiment(experiment);
        return experimentId;
    }

    public Experiment getExperiment(String experimentId) throws IOException {
        log.debug("Fetching experiment: {}", experimentId);
        Experiment experiment = experimentRepository.getExperiment(experimentId);
        if (experiment != null) {
            Map<String, String> tagsMap = securityContextHelper.tagsToMap(
                    experiment.getTags(), ExperimentTag::getKey, ExperimentTag::getValue);
            securityContextHelper.checkReadAccess(RESOURCE_TYPE, experimentId, experiment.getOwner(), tagsMap);
        }
        return experiment;
    }

    public Experiment getExperimentByName(String name) throws IOException {
        Experiment experiment = experimentRepository.getExperimentByName(name);
        if (experiment != null) {
            Map<String, String> tagsMap = securityContextHelper.tagsToMap(
                    experiment.getTags(), ExperimentTag::getKey, ExperimentTag::getValue);
            securityContextHelper.checkReadAccess(RESOURCE_TYPE, experiment.getExperimentId(), experiment.getOwner(), tagsMap);
        }
        return experiment;
    }

    public void updateExperiment(String experimentId, String newName) throws IOException {
        Experiment experiment = experimentRepository.getExperiment(experimentId);
        if (experiment != null) {
            Map<String, String> tagsMap = securityContextHelper.tagsToMap(
                    experiment.getTags(), ExperimentTag::getKey, ExperimentTag::getValue);
            securityContextHelper.checkWriteAccess(RESOURCE_TYPE, experimentId, experiment.getOwner(), tagsMap);
        }
        experimentRepository.updateExperiment(experimentId, newName);
    }

    public void deleteExperiment(String experimentId) throws IOException {
        Experiment experiment = experimentRepository.getExperiment(experimentId);
        if (experiment != null) {
            Map<String, String> tagsMap = securityContextHelper.tagsToMap(
                    experiment.getTags(), ExperimentTag::getKey, ExperimentTag::getValue);
            securityContextHelper.checkDeleteAccess(RESOURCE_TYPE, experimentId, experiment.getOwner(), tagsMap);
        }
        experimentRepository.deleteExperiment(experimentId);
    }

    public void restoreExperiment(String experimentId) throws IOException {
        Experiment experiment = experimentRepository.getExperiment(experimentId);
        if (experiment != null) {
            Map<String, String> tagsMap = securityContextHelper.tagsToMap(
                    experiment.getTags(), ExperimentTag::getKey, ExperimentTag::getValue);
            securityContextHelper.checkWriteAccess(RESOURCE_TYPE, experimentId, experiment.getOwner(), tagsMap);
        }
        experimentRepository.restoreExperiment(experimentId);
    }

    public void setExperimentTag(String experimentId, String key, String value) throws IOException {
        Experiment experiment = experimentRepository.getExperiment(experimentId);
        if (experiment != null) {
            Map<String, String> tagsMap = securityContextHelper.tagsToMap(
                    experiment.getTags(), ExperimentTag::getKey, ExperimentTag::getValue);
            securityContextHelper.checkWriteAccess(RESOURCE_TYPE, experimentId, experiment.getOwner(), tagsMap);
        }
        experimentRepository.setExperimentTag(experimentId, key, value);
    }

    public List<Experiment> listExperiments() throws IOException {
        List<Experiment> experiments = experimentRepository.listExperiments();
        // Filter experiments based on read access
        return experiments.stream()
                .filter(exp -> {
                    Map<String, String> tagsMap = securityContextHelper.tagsToMap(
                            exp.getTags(), ExperimentTag::getKey, ExperimentTag::getValue);
                    return securityContextHelper.canRead(RESOURCE_TYPE, exp.getExperimentId(), exp.getOwner(), tagsMap);
                })
                .collect(Collectors.toList());
    }

    public List<Experiment> searchExperiments(String viewType, Integer maxResults, String filter) throws IOException {
        List<Experiment> all = experimentRepository.listExperiments();
        
        // Filter by read access first
        List<Experiment> accessible = all.stream()
            .filter(exp -> {
                Map<String, String> tagsMap = securityContextHelper.tagsToMap(
                        exp.getTags(), ExperimentTag::getKey, ExperimentTag::getValue);
                return securityContextHelper.canRead(RESOURCE_TYPE, exp.getExperimentId(), exp.getOwner(), tagsMap);
            })
            .collect(Collectors.toList());
        
        // Filter by lifecycle_stage
        List<Experiment> filtered = accessible.stream()
            .filter(e -> {
                if ("ACTIVE_ONLY".equalsIgnoreCase(viewType) || viewType == null) {
                    return "active".equalsIgnoreCase(e.getLifecycleStage());
                } else if ("DELETED_ONLY".equalsIgnoreCase(viewType)) {
                    return "deleted".equalsIgnoreCase(e.getLifecycleStage());
                }
                return true; // ViewType.ALL
            })
            .collect(Collectors.toList());

        // Simple name filtering (MLFlow supports a simple query language)
        if (filter != null && !filter.isEmpty()) {
            // Simple example: filter="name = 'my_exp'"
            if (filter.contains("name =")) {
                String targetName = filter.split("=")[1].trim().replace("'", "");
                filtered = filtered.stream()
                    .filter(e -> e.getName().equals(targetName))
                    .collect(Collectors.toList());
            }
        }

        if (maxResults != null && maxResults > 0 && filtered.size() > maxResults) {
            return filtered.subList(0, maxResults);
        }
        
        return filtered;
    }
}
