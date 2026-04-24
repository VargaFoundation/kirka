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

        if (filter != null && !filter.isEmpty()) {
            if (filter.contains("name LIKE")) {
                String pattern = filter.split("LIKE")[1].trim().replace("'", "").replace("%", ".*");
                return accessible.stream()
                    .filter(m -> m.getName().matches(pattern))
                    .collect(Collectors.toList());
            }
        }
        return accessible;
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

    /** Server-side search on model versions, with optional name and stage filters. */
    public List<ModelVersion> searchModelVersions(String filter, int maxResults) throws IOException {
        String modelName = null;
        String stageFilter = null;
        if (filter != null && !filter.isBlank()) {
            // Very small DSL: "name = 'X'" and/or "current_stage = 'Production'".
            for (String clause : filter.split(" and ")) {
                String trimmed = clause.trim();
                if (trimmed.startsWith("name")) {
                    modelName = extractQuoted(trimmed);
                } else if (trimmed.startsWith("current_stage")) {
                    stageFilter = extractQuoted(trimmed);
                }
            }
        }
        int cap = Math.max(1, Math.min(maxResults <= 0 ? 1000 : maxResults, 10000));
        List<ModelVersion> versions = modelRegistryRepository.searchModelVersions(modelName, stageFilter, cap);

        // Authorization filter: we can only return versions whose parent model is visible.
        List<ModelVersion> accessible = new java.util.ArrayList<>(versions.size());
        for (ModelVersion mv : versions) {
            RegisteredModel parent = modelRegistryRepository.getRegisteredModel(mv.getName());
            if (parent == null) continue;
            Map<String, String> tagsMap = getModelTagsMap(parent);
            if (securityContextHelper.canRead(RESOURCE_TYPE, parent.getName(), parent.getUserId(), tagsMap)) {
                accessible.add(mv);
            }
        }
        return accessible;
    }

    private static String extractQuoted(String clause) {
        int first = clause.indexOf('\'');
        int last = clause.lastIndexOf('\'');
        if (first < 0 || last <= first) return null;
        return clause.substring(first + 1, last);
    }

    private Map<String, String> getModelTagsMap(RegisteredModel model) {
        if (model.getTags() == null) {
            return Map.of();
        }
        return securityContextHelper.tagsToMap(model.getTags(), RegisteredModelTag::getKey, RegisteredModelTag::getValue);
    }
}
