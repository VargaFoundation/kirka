package varga.kirka.service;

import varga.kirka.model.ModelVersion;
import varga.kirka.model.RegisteredModel;
import varga.kirka.repo.ModelRegistryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Service
public class ModelRegistryService {

    @Autowired
    private ModelRegistryRepository modelRegistryRepository;

    public void createRegisteredModel(String name) throws IOException {
        modelRegistryRepository.createRegisteredModel(name);
    }

    public RegisteredModel getRegisteredModel(String name) throws IOException {
        return modelRegistryRepository.getRegisteredModel(name);
    }

    public List<RegisteredModel> listRegisteredModels() throws IOException {
        return modelRegistryRepository.listRegisteredModels();
    }

    public ModelVersion createModelVersion(String name, String source, String runId) throws IOException {
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
                .build();
        
        modelRegistryRepository.createModelVersion(version);
        return version;
    }
    public List<RegisteredModel> searchRegisteredModels(String filter) throws IOException {
        // Basic implementation: list all and filter if needed
        return modelRegistryRepository.listRegisteredModels();
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
}
