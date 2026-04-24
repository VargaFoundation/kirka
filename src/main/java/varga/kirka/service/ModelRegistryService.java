package varga.kirka.service;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.*;
import varga.kirka.repo.ModelRegistryRepository;
import varga.kirka.security.SecurityContextHelper;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ModelRegistryService {

    private static final String RESOURCE_TYPE = "model";

    private final ModelRegistryRepository modelRegistryRepository;

    private final SecurityContextHelper securityContextHelper;

    public void createRegisteredModel(String name) throws IOException {
        log.info("Creating registered model: {}", name);
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Model name must not be empty");
        }
        modelRegistryRepository.createRegisteredModel(name);
    }

    public RegisteredModel getRegisteredModel(String name) throws IOException {
        log.debug("Fetching registered model: {}", name);
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model == null) {
            throw new ResourceNotFoundException("RegisteredModel", name);
        }
        Map<String, String> tagsMap = getModelTagsMap(model);
        securityContextHelper.checkReadAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        return model;
    }

    public List<RegisteredModel> listRegisteredModels() throws IOException {
        log.debug("Listing registered models");
        List<RegisteredModel> models = modelRegistryRepository.listRegisteredModels();
        return models.stream()
                .filter(model -> {
                    Map<String, String> tagsMap = getModelTagsMap(model);
                    return securityContextHelper.canRead(RESOURCE_TYPE, model.getName(), model.getUserId(), tagsMap);
                })
                .collect(Collectors.toList());
    }

    /** Paginated registered-model listing with Ranger read filtering applied per page. */
    public varga.kirka.repo.Page<RegisteredModel> listRegisteredModelsPaged(Integer maxResults, String pageToken) throws IOException {
        int pageSize = varga.kirka.repo.PageToken.clampPageSize(maxResults);
        varga.kirka.repo.PageToken token = varga.kirka.repo.PageToken.decode(pageToken);
        varga.kirka.repo.Page<RegisteredModel> raw = modelRegistryRepository.listRegisteredModelsPaged(pageSize, token);
        if (raw == null) return new varga.kirka.repo.Page<>(List.of(), null);
        List<RegisteredModel> filtered = raw.items().stream()
                .filter(model -> {
                    Map<String, String> tagsMap = getModelTagsMap(model);
                    return securityContextHelper.canRead(RESOURCE_TYPE, model.getName(), model.getUserId(), tagsMap);
                })
                .collect(Collectors.toList());
        return new varga.kirka.repo.Page<>(filtered, raw.nextPageToken());
    }

    public ModelVersion createModelVersion(String name, String source, String runId) throws IOException {
        log.info("Creating model version for: {}, source: {}, runId: {}", name, source, runId);
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model == null) {
            throw new ResourceNotFoundException("RegisteredModel", name);
        }
        Map<String, String> tagsMap = getModelTagsMap(model);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);

        int versionNum = 1;
        if (model.getLatestVersions() != null) {
            versionNum = model.getLatestVersions().size() + 1;
        }

        ModelVersion version = ModelVersion.builder()
                .name(name)
                .version(String.valueOf(versionNum))
                .creationTimestamp(System.currentTimeMillis())
                .currentStage("None")
                .source(source)
                .runId(runId)
                .status(ModelVersionStatus.READY)
                .build();

        modelRegistryRepository.createModelVersion(version);
        return version;
    }

    public List<RegisteredModel> searchRegisteredModels(String filter) throws IOException {
        List<RegisteredModel> all = modelRegistryRepository.listRegisteredModels();

        List<RegisteredModel> accessible = all.stream()
                .filter(model -> {
                    Map<String, String> tagsMap = getModelTagsMap(model);
                    return securityContextHelper.canRead(RESOURCE_TYPE, model.getName(), model.getUserId(), tagsMap);
                })
                .collect(Collectors.toList());

        if (filter != null && !filter.isBlank()) {
            var clauses = varga.kirka.search.FilterParser.parse(filter);
            var evaluator = modelFilterEvaluator();
            accessible = accessible.stream()
                    .filter(m -> evaluator.matches(m, clauses))
                    .collect(Collectors.toList());
        }
        return accessible;
    }

    private static varga.kirka.search.FilterEvaluator<RegisteredModel> modelFilterEvaluator() {
        return new varga.kirka.search.FilterEvaluator<>(
                ModelRegistryService::modelTagsAsMap,
                m -> Map.of(),
                m -> Map.of(),
                ModelRegistryService::modelAttribute);
    }

    private static Map<String, String> modelTagsAsMap(RegisteredModel m) {
        if (m.getTags() == null) return Map.of();
        Map<String, String> out = new java.util.HashMap<>(m.getTags().size());
        for (RegisteredModelTag t : m.getTags()) out.put(t.getKey(), t.getValue());
        return out;
    }

    private static Object modelAttribute(RegisteredModel m, String name) {
        return switch (name) {
            case "name", "id" -> m.getName();
            case "creation_timestamp", "creation_time" -> m.getCreationTimestamp();
            case "last_updated_timestamp", "last_update_time" -> m.getLastUpdatedTimestamp();
            case "description" -> m.getDescription();
            case "user_id", "owner" -> m.getUserId();
            default -> null;
        };
    }

    public void updateRegisteredModel(String name, String description) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model == null) {
            throw new ResourceNotFoundException("RegisteredModel", name);
        }
        Map<String, String> tagsMap = getModelTagsMap(model);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        modelRegistryRepository.updateRegisteredModel(name, description);
    }

    public void deleteRegisteredModel(String name) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model == null) {
            throw new ResourceNotFoundException("RegisteredModel", name);
        }
        Map<String, String> tagsMap = getModelTagsMap(model);
        securityContextHelper.checkDeleteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        modelRegistryRepository.deleteRegisteredModel(name);
    }

    public ModelVersion getModelVersion(String name, String version) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model == null) {
            throw new ResourceNotFoundException("RegisteredModel", name);
        }
        Map<String, String> tagsMap = getModelTagsMap(model);
        securityContextHelper.checkReadAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        ModelVersion mv = modelRegistryRepository.getModelVersion(name, version);
        if (mv == null) {
            throw new ResourceNotFoundException("ModelVersion", name + "/" + version);
        }
        return mv;
    }

    public void updateModelVersion(String name, String version, String description) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model == null) {
            throw new ResourceNotFoundException("RegisteredModel", name);
        }
        Map<String, String> tagsMap = getModelTagsMap(model);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        modelRegistryRepository.updateModelVersion(name, version, description);
    }

    public void deleteModelVersion(String name, String version) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model == null) {
            throw new ResourceNotFoundException("RegisteredModel", name);
        }
        Map<String, String> tagsMap = getModelTagsMap(model);
        securityContextHelper.checkDeleteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        modelRegistryRepository.deleteModelVersion(name, version);
    }

    public ModelVersion transitionModelVersionStage(String name, String version, String stage, boolean archiveExistingVersions) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model == null) {
            throw new ResourceNotFoundException("RegisteredModel", name);
        }
        Map<String, String> tagsMap = getModelTagsMap(model);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);

        if (archiveExistingVersions) {
            List<ModelVersion> versions = modelRegistryRepository.getVersions(name);
            for (ModelVersion v : versions) {
                if (v.getCurrentStage().equalsIgnoreCase(stage) && !v.getVersion().equals(version)) {
                    modelRegistryRepository.updateModelVersionStage(name, v.getVersion(), "Archived");
                }
            }
        }
        modelRegistryRepository.updateModelVersionStage(name, version, stage);
        return modelRegistryRepository.getModelVersion(name, version);
    }

    public void setRegisteredModelTag(String name, String key, String value) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model == null) {
            throw new ResourceNotFoundException("RegisteredModel", name);
        }
        Map<String, String> tagsMap = getModelTagsMap(model);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        modelRegistryRepository.setRegisteredModelTag(name, key, value);
    }

    public void deleteRegisteredModelTag(String name, String key) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model == null) {
            throw new ResourceNotFoundException("RegisteredModel", name);
        }
        Map<String, String> tagsMap = getModelTagsMap(model);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        modelRegistryRepository.deleteRegisteredModelTag(name, key);
    }

    public void setModelVersionTag(String name, String version, String key, String value) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model == null) {
            throw new ResourceNotFoundException("RegisteredModel", name);
        }
        Map<String, String> tagsMap = getModelTagsMap(model);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        modelRegistryRepository.setModelVersionTag(name, version, key, value);
    }

    public void deleteModelVersionTag(String name, String version, String key) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model == null) {
            throw new ResourceNotFoundException("RegisteredModel", name);
        }
        Map<String, String> tagsMap = getModelTagsMap(model);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        modelRegistryRepository.deleteModelVersionTag(name, version, key);
    }

    /** Sets an alias on a registered model. The target version must exist. */
    public void setAlias(String name, String alias, String version) throws IOException {
        if (alias == null || alias.isBlank()) {
            throw new IllegalArgumentException("Alias must not be empty");
        }
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model == null) {
            throw new ResourceNotFoundException("RegisteredModel", name);
        }
        Map<String, String> tagsMap = getModelTagsMap(model);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);

        ModelVersion target = modelRegistryRepository.getModelVersion(name, version);
        if (target == null) {
            throw new ResourceNotFoundException("ModelVersion", name + "/" + version);
        }
        modelRegistryRepository.setAlias(name, alias, version);
    }

    public void deleteAlias(String name, String alias) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model == null) {
            throw new ResourceNotFoundException("RegisteredModel", name);
        }
        Map<String, String> tagsMap = getModelTagsMap(model);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        modelRegistryRepository.deleteAlias(name, alias);
    }

    /** Resolves an alias to its pinned model version. */
    public ModelVersion getModelVersionByAlias(String name, String alias) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model == null) {
            throw new ResourceNotFoundException("RegisteredModel", name);
        }
        Map<String, String> tagsMap = getModelTagsMap(model);
        securityContextHelper.checkReadAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        String version = modelRegistryRepository.getAliasedVersion(name, alias);
        if (version == null) {
            throw new ResourceNotFoundException("Alias", name + "@" + alias);
        }
        ModelVersion mv = modelRegistryRepository.getModelVersion(name, version);
        if (mv == null) {
            throw new ResourceNotFoundException("ModelVersion", name + "/" + version);
        }
        return mv;
    }

    /** Renames a registered model. Fails if a model with {@code newName} already exists. */
    public void renameRegisteredModel(String oldName, String newName) throws IOException {
        if (newName == null || newName.isBlank()) {
            throw new IllegalArgumentException("new_name must not be empty");
        }
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(oldName);
        if (model == null) {
            throw new ResourceNotFoundException("RegisteredModel", oldName);
        }
        Map<String, String> tagsMap = getModelTagsMap(model);
        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, oldName, model.getUserId(), tagsMap);
        modelRegistryRepository.renameRegisteredModel(oldName, newName);
    }

    /**
     * Returns the URI at which the artifact of a given model version can be downloaded. For
     * models registered from a run, this is the run's artifact location; otherwise it falls
     * back to the raw {@code source} recorded at registration time.
     */
    public String getModelVersionDownloadUri(String name, String version) throws IOException {
        ModelVersion mv = getModelVersion(name, version);
        // getModelVersion already raised ResourceNotFoundException if absent and enforced authz.
        String source = mv.getSource();
        if (source == null || source.isBlank()) {
            throw new ResourceNotFoundException("ModelVersionSource", name + "/" + version);
        }
        return source;
    }

    /**
     * Server-side search on model versions. Accepts MLFlow filter syntax — the parser is the
     * same as for experiments and runs. When the filter contains a top-level
     * {@code name = '...'} equality we push it down as an HBase prefix scan on the versions
     * table to avoid scanning every version of every model.
     */
    public List<ModelVersion> searchModelVersions(String filter, int maxResults) throws IOException {
        List<varga.kirka.search.FilterClause> clauses = varga.kirka.search.FilterParser.parse(filter);
        String modelNamePrefix = extractEqualityAttribute(clauses, "name");

        int cap = Math.max(1, Math.min(maxResults <= 0 ? 1000 : maxResults, 10000));
        // `stageFilter` is a cheap server-side short-circuit; the parser still evaluates the
        // full expression below so other predicates are honoured.
        String stageFilter = extractEqualityAttribute(clauses, "current_stage");
        List<ModelVersion> versions = modelRegistryRepository.searchModelVersions(modelNamePrefix, stageFilter, cap);

        var evaluator = modelVersionFilterEvaluator();
        List<ModelVersion> accessible = new java.util.ArrayList<>(versions.size());
        for (ModelVersion mv : versions) {
            if (!evaluator.matches(mv, clauses)) continue;
            RegisteredModel parent = modelRegistryRepository.getRegisteredModel(mv.getName());
            if (parent == null) continue;
            Map<String, String> tagsMap = getModelTagsMap(parent);
            if (securityContextHelper.canRead(RESOURCE_TYPE, parent.getName(), parent.getUserId(), tagsMap)) {
                accessible.add(mv);
            }
        }
        return accessible;
    }

    /** Returns the string value of a top-level {@code attribute = '...'} clause, or null. */
    private static String extractEqualityAttribute(List<varga.kirka.search.FilterClause> clauses, String key) {
        for (var c : clauses) {
            if (c.field() == varga.kirka.search.FilterClause.Field.ATTRIBUTE
                    && c.op() == varga.kirka.search.FilterClause.Operator.EQ
                    && key.equalsIgnoreCase(c.key())
                    && c.firstValue() != null) {
                return String.valueOf(c.firstValue());
            }
        }
        return null;
    }

    private static varga.kirka.search.FilterEvaluator<ModelVersion> modelVersionFilterEvaluator() {
        return new varga.kirka.search.FilterEvaluator<>(
                mv -> mv.getTags() != null
                        ? mv.getTags().stream().collect(java.util.stream.Collectors.toMap(
                            ModelVersionTag::getKey, ModelVersionTag::getValue, (a, b) -> b))
                        : Map.of(),
                mv -> Map.of(),
                mv -> Map.of(),
                ModelRegistryService::modelVersionAttribute);
    }

    private static Object modelVersionAttribute(ModelVersion mv, String name) {
        return switch (name) {
            case "name" -> mv.getName();
            case "version" -> mv.getVersion();
            case "current_stage" -> mv.getCurrentStage();
            case "source" -> mv.getSource();
            case "run_id" -> mv.getRunId();
            case "status" -> mv.getStatus() != null ? mv.getStatus().name() : null;
            case "creation_timestamp", "creation_time" -> mv.getCreationTimestamp();
            case "last_updated_timestamp", "last_update_time" -> mv.getLastUpdatedTimestamp();
            case "description" -> mv.getDescription();
            case "user_id" -> mv.getUserId();
            default -> null;
        };
    }

    private Map<String, String> getModelTagsMap(RegisteredModel model) {
        if (model.getTags() == null) {
            return Map.of();
        }
        return securityContextHelper.tagsToMap(model.getTags(), RegisteredModelTag::getKey, RegisteredModelTag::getValue);
    }
}
