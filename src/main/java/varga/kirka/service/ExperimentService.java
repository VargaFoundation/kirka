package varga.kirka.service;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.*;
import varga.kirka.repo.ExperimentRepository;
import varga.kirka.search.FilterClause;
import varga.kirka.search.FilterEvaluator;
import varga.kirka.search.FilterParser;
import varga.kirka.security.SecurityContextHelper;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ExperimentService {

    private static final String RESOURCE_TYPE = "experiment";

    private final ExperimentRepository experimentRepository;

    private final SecurityContextHelper securityContextHelper;

    public String createExperiment(String name, String artifactLocation, List<ExperimentTag> tags) throws IOException {
        log.info("Creating experiment with name: {}", name);
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Experiment name must not be empty");
        }
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
        if (experiment == null) {
            throw new ResourceNotFoundException("Experiment", experimentId);
        }
        Map<String, String> tagsMap = securityContextHelper.tagsToMap(
                experiment.getTags(), ExperimentTag::getKey, ExperimentTag::getValue);
        securityContextHelper.checkReadAccess(RESOURCE_TYPE, experimentId, experiment.getOwner(), tagsMap);
        return experiment;
    }

    public Experiment getExperimentByName(String name) throws IOException {
        Experiment experiment = experimentRepository.getExperimentByName(name);
        if (experiment == null) {
            throw new ResourceNotFoundException("Experiment", name);
        }
        Map<String, String> tagsMap = securityContextHelper.tagsToMap(
                experiment.getTags(), ExperimentTag::getKey, ExperimentTag::getValue);
        securityContextHelper.checkReadAccess(RESOURCE_TYPE, experiment.getExperimentId(), experiment.getOwner(), tagsMap);
        return experiment;
    }

    public void updateExperiment(String experimentId, String newName) throws IOException {
        Experiment experiment = experimentRepository.getExperiment(experimentId);
        if (experiment == null) {
            throw new ResourceNotFoundException("Experiment", experimentId);
        }
        Map<String, String> tagsMap = securityContextHelper.tagsToMap(
                experiment.getTags(), ExperimentTag::getKey, ExperimentTag::getValue);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, experimentId, experiment.getOwner(), tagsMap);
        experimentRepository.updateExperiment(experimentId, newName);
    }

    public void deleteExperiment(String experimentId) throws IOException {
        Experiment experiment = experimentRepository.getExperiment(experimentId);
        if (experiment == null) {
            throw new ResourceNotFoundException("Experiment", experimentId);
        }
        Map<String, String> tagsMap = securityContextHelper.tagsToMap(
                experiment.getTags(), ExperimentTag::getKey, ExperimentTag::getValue);
        securityContextHelper.checkDeleteAccess(RESOURCE_TYPE, experimentId, experiment.getOwner(), tagsMap);
        experimentRepository.deleteExperiment(experimentId);
    }

    public void restoreExperiment(String experimentId) throws IOException {
        Experiment experiment = experimentRepository.getExperiment(experimentId);
        if (experiment == null) {
            throw new ResourceNotFoundException("Experiment", experimentId);
        }
        Map<String, String> tagsMap = securityContextHelper.tagsToMap(
                experiment.getTags(), ExperimentTag::getKey, ExperimentTag::getValue);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, experimentId, experiment.getOwner(), tagsMap);
        experimentRepository.restoreExperiment(experimentId);
    }

    public void setExperimentTag(String experimentId, String key, String value) throws IOException {
        Experiment experiment = experimentRepository.getExperiment(experimentId);
        if (experiment == null) {
            throw new ResourceNotFoundException("Experiment", experimentId);
        }
        Map<String, String> tagsMap = securityContextHelper.tagsToMap(
                experiment.getTags(), ExperimentTag::getKey, ExperimentTag::getValue);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, experimentId, experiment.getOwner(), tagsMap);
        experimentRepository.setExperimentTag(experimentId, key, value);
    }

    public List<Experiment> listExperiments() throws IOException {
        List<Experiment> experiments = experimentRepository.listExperiments();
        return experiments.stream()
                .filter(exp -> {
                    Map<String, String> tagsMap = securityContextHelper.tagsToMap(
                            exp.getTags(), ExperimentTag::getKey, ExperimentTag::getValue);
                    return securityContextHelper.canRead(RESOURCE_TYPE, exp.getExperimentId(), exp.getOwner(), tagsMap);
                })
                .collect(Collectors.toList());
    }

    /**
     * Paginated experiment listing. Authorization filtering is applied per page, which means a
     * particularly restrictive Ranger policy can produce short pages; callers should keep
     * paging while {@code nextPageToken} is non-null rather than assume a full page.
     */
    public varga.kirka.repo.Page<Experiment> listExperimentsPaged(Integer maxResults, String pageToken) throws IOException {
        int pageSize = varga.kirka.repo.PageToken.clampPageSize(maxResults);
        varga.kirka.repo.PageToken token = varga.kirka.repo.PageToken.decode(pageToken);
        varga.kirka.repo.Page<Experiment> raw = experimentRepository.listExperimentsPaged(pageSize, token);
        if (raw == null) return new varga.kirka.repo.Page<>(List.of(), null);
        List<Experiment> filtered = raw.items().stream()
                .filter(exp -> {
                    Map<String, String> tagsMap = securityContextHelper.tagsToMap(
                            exp.getTags(), ExperimentTag::getKey, ExperimentTag::getValue);
                    return securityContextHelper.canRead(RESOURCE_TYPE, exp.getExperimentId(), exp.getOwner(), tagsMap);
                })
                .collect(Collectors.toList());
        return new varga.kirka.repo.Page<>(filtered, raw.nextPageToken());
    }

    public List<Experiment> searchExperiments(String viewType, Integer maxResults, String filter) throws IOException {
        List<Experiment> all = experimentRepository.listExperiments();

        List<Experiment> accessible = all.stream()
            .filter(exp -> {
                Map<String, String> tagsMap = securityContextHelper.tagsToMap(
                        exp.getTags(), ExperimentTag::getKey, ExperimentTag::getValue);
                return securityContextHelper.canRead(RESOURCE_TYPE, exp.getExperimentId(), exp.getOwner(), tagsMap);
            })
            .collect(Collectors.toList());

        List<Experiment> filtered = accessible.stream()
            .filter(e -> matchesViewType(e, viewType))
            .collect(Collectors.toList());

        if (filter != null && !filter.isBlank()) {
            List<FilterClause> clauses = FilterParser.parse(filter);
            FilterEvaluator<Experiment> evaluator = experimentFilterEvaluator();
            filtered = filtered.stream()
                    .filter(e -> evaluator.matches(e, clauses))
                    .collect(Collectors.toList());
        }

        if (maxResults != null && maxResults > 0 && filtered.size() > maxResults) {
            return filtered.subList(0, maxResults);
        }
        return filtered;
    }

    private static boolean matchesViewType(Experiment e, String viewType) {
        if ("ACTIVE_ONLY".equalsIgnoreCase(viewType) || viewType == null) {
            return "active".equalsIgnoreCase(e.getLifecycleStage());
        }
        if ("DELETED_ONLY".equalsIgnoreCase(viewType)) {
            return "deleted".equalsIgnoreCase(e.getLifecycleStage());
        }
        return true; // ALL
    }

    private static FilterEvaluator<Experiment> experimentFilterEvaluator() {
        return new FilterEvaluator<>(
                ExperimentService::tagsAsMap,
                e -> Map.of(),
                e -> Map.of(),
                ExperimentService::attribute);
    }

    private static Map<String, String> tagsAsMap(Experiment e) {
        if (e.getTags() == null) return Map.of();
        Map<String, String> m = new HashMap<>(e.getTags().size());
        for (ExperimentTag t : e.getTags()) m.put(t.getKey(), t.getValue());
        return m;
    }

    private static Object attribute(Experiment e, String name) {
        return switch (name) {
            case "experiment_id", "id" -> e.getExperimentId();
            case "name" -> e.getName();
            case "artifact_location" -> e.getArtifactLocation();
            case "lifecycle_stage" -> e.getLifecycleStage();
            case "creation_time" -> e.getCreationTime();
            case "last_update_time" -> e.getLastUpdateTime();
            case "owner", "user_id" -> e.getOwner();
            default -> null;
        };
    }
}
