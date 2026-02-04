package varga.kirka.service;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.*;
import varga.kirka.repo.ModelRegistryRepository;
import varga.kirka.security.SecurityContextHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ModelRegistryService {

    private static final String RESOURCE_TYPE = "model";

    @Autowired
    private ModelRegistryRepository modelRegistryRepository;

    @Autowired
    private SecurityContextHelper securityContextHelper;

    public void createRegisteredModel(String name) throws IOException {
        log.info("Creating registered model: {}", name);
        modelRegistryRepository.createRegisteredModel(name);
    }

    public RegisteredModel getRegisteredModel(String name) throws IOException {
        log.debug("Fetching registered model: {}", name);
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model != null) {
            Map<String, String> tagsMap = getModelTagsMap(model);
            securityContextHelper.checkReadAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        }
        return model;
    }

    public List<RegisteredModel> listRegisteredModels() throws IOException {
        log.debug("Listing registered models");
        List<RegisteredModel> models = modelRegistryRepository.listRegisteredModels();
        // Filter models based on read access
        return models.stream()
                .filter(model -> {
                    Map<String, String> tagsMap = getModelTagsMap(model);
                    return securityContextHelper.canRead(RESOURCE_TYPE, model.getName(), model.getUserId(), tagsMap);
                })
                .collect(Collectors.toList());
    }

    public ModelVersion createModelVersion(String name, String source, String runId) throws IOException {
        log.info("Creating model version for: {}, source: {}, runId: {}", name, source, runId);
        // Check write access on the model
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model != null) {
            Map<String, String> tagsMap = getModelTagsMap(model);
            securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        }
        
        // Simple versioning logic: get existing and increment
        int versionNum = 1;
        if (model != null && model.getLatestVersions() != null) {
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
        
        // Filter by read access first
        List<RegisteredModel> accessible = all.stream()
                .filter(model -> {
                    Map<String, String> tagsMap = getModelTagsMap(model);
                    return securityContextHelper.canRead(RESOURCE_TYPE, model.getName(), model.getUserId(), tagsMap);
                })
                .collect(Collectors.toList());
        
        if (filter != null && !filter.isEmpty()) {
            // MLFlow filter for models usually looks like "name LIKE 'my_model%'"
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
        if (model != null) {
            Map<String, String> tagsMap = getModelTagsMap(model);
            securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        }
        modelRegistryRepository.updateRegisteredModel(name, description);
    }

    public void deleteRegisteredModel(String name) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model != null) {
            Map<String, String> tagsMap = getModelTagsMap(model);
            securityContextHelper.checkDeleteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        }
        modelRegistryRepository.deleteRegisteredModel(name);
    }

    public ModelVersion getModelVersion(String name, String version) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model != null) {
            Map<String, String> tagsMap = getModelTagsMap(model);
            securityContextHelper.checkReadAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        }
        return modelRegistryRepository.getModelVersion(name, version);
    }

    public void updateModelVersion(String name, String version, String description) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model != null) {
            Map<String, String> tagsMap = getModelTagsMap(model);
            securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        }
        modelRegistryRepository.updateModelVersion(name, version, description);
    }

    public void deleteModelVersion(String name, String version) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model != null) {
            Map<String, String> tagsMap = getModelTagsMap(model);
            securityContextHelper.checkDeleteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        }
        modelRegistryRepository.deleteModelVersion(name, version);
    }

    public ModelVersion transitionModelVersionStage(String name, String version, String stage, boolean archiveExistingVersions) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model != null) {
            Map<String, String> tagsMap = getModelTagsMap(model);
            securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        }
        
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
        if (model != null) {
            Map<String, String> tagsMap = getModelTagsMap(model);
            securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        }
        modelRegistryRepository.setRegisteredModelTag(name, key, value);
    }

    public void deleteRegisteredModelTag(String name, String key) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model != null) {
            Map<String, String> tagsMap = getModelTagsMap(model);
            securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        }
        modelRegistryRepository.deleteRegisteredModelTag(name, key);
    }

    public void setModelVersionTag(String name, String version, String key, String value) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model != null) {
            Map<String, String> tagsMap = getModelTagsMap(model);
            securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        }
        modelRegistryRepository.setModelVersionTag(name, version, key, value);
    }

    public void deleteModelVersionTag(String name, String version, String key) throws IOException {
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
        if (model != null) {
            Map<String, String> tagsMap = getModelTagsMap(model);
            securityContextHelper.checkWriteAccess(RESOURCE_TYPE, name, model.getUserId(), tagsMap);
        }
        modelRegistryRepository.deleteModelVersionTag(name, version, key);
    }

    /**
     * Extracts tags from a RegisteredModel as a Map for authorization checks.
     */
    private Map<String, String> getModelTagsMap(RegisteredModel model) {
        if (model.getTags() == null) {
            return Map.of();
        }
        return securityContextHelper.tagsToMap(model.getTags(), RegisteredModelTag::getKey, RegisteredModelTag::getValue);
    }
}
