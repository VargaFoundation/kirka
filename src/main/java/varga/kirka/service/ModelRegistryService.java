package varga.kirka.service;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.*;
import varga.kirka.repo.ModelRegistryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ModelRegistryService {

    @Autowired
    private ModelRegistryRepository modelRegistryRepository;

    public void createRegisteredModel(String name) throws IOException {
        log.info("Creating registered model: {}", name);
        modelRegistryRepository.createRegisteredModel(name);
    }

    public RegisteredModel getRegisteredModel(String name) throws IOException {
        log.debug("Fetching registered model: {}", name);
        return modelRegistryRepository.getRegisteredModel(name);
    }

    public List<RegisteredModel> listRegisteredModels() throws IOException {
        log.debug("Listing registered models");
        return modelRegistryRepository.listRegisteredModels();
    }

    public ModelVersion createModelVersion(String name, String source, String runId) throws IOException {
        log.info("Creating model version for: {}, source: {}, runId: {}", name, source, runId);
        // Simple versioning logic: get existing and increment
        RegisteredModel model = modelRegistryRepository.getRegisteredModel(name);
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
        if (filter != null && !filter.isEmpty()) {
            // MLFlow filter for models usually looks like "name LIKE 'my_model%'"
            if (filter.contains("name LIKE")) {
                String pattern = filter.split("LIKE")[1].trim().replace("'", "").replace("%", ".*");
                return all.stream()
                    .filter(m -> m.getName().matches(pattern))
                    .collect(Collectors.toList());
            }
        }
        return all;
    }

    public void updateRegisteredModel(String name, String description) throws IOException {
        modelRegistryRepository.updateRegisteredModel(name, description);
    }

    public void deleteRegisteredModel(String name) throws IOException {
        modelRegistryRepository.deleteRegisteredModel(name);
    }

    public ModelVersion getModelVersion(String name, String version) throws IOException {
        return modelRegistryRepository.getModelVersion(name, version);
    }

    public void updateModelVersion(String name, String version, String description) throws IOException {
        modelRegistryRepository.updateModelVersion(name, version, description);
    }

    public void deleteModelVersion(String name, String version) throws IOException {
        modelRegistryRepository.deleteModelVersion(name, version);
    }

    public ModelVersion transitionModelVersionStage(String name, String version, String stage, boolean archiveExistingVersions) throws IOException {
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
        modelRegistryRepository.setRegisteredModelTag(name, key, value);
    }

    public void deleteRegisteredModelTag(String name, String key) throws IOException {
        modelRegistryRepository.deleteRegisteredModelTag(name, key);
    }

    public void setModelVersionTag(String name, String version, String key, String value) throws IOException {
        modelRegistryRepository.setModelVersionTag(name, version, key, value);
    }

    public void deleteModelVersionTag(String name, String version, String key) throws IOException {
        modelRegistryRepository.deleteModelVersionTag(name, version, key);
    }
}
