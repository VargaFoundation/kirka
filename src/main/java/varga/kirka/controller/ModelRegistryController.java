package varga.kirka.controller;

import varga.kirka.model.ModelVersion;
import varga.kirka.model.RegisteredModel;
import varga.kirka.service.ModelRegistryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;

@RestController
@RequestMapping("/api/2.0/mlflow")
public class ModelRegistryController {

    @Autowired
    private ModelRegistryService modelRegistryService;

    @PostMapping("/registered-models/create")
    public Map<String, Object> createRegisteredModel(@RequestBody Map<String, String> request) throws IOException {
        String name = request.get("name");
        modelRegistryService.createRegisteredModel(name);
        return Map.of("registered_model", modelRegistryService.getRegisteredModel(name));
    }

    @GetMapping("/registered-models/get")
    public Map<String, Object> getRegisteredModel(@RequestParam("name") String name) throws IOException {
        return Map.of("registered_model", modelRegistryService.getRegisteredModel(name));
    }

    @GetMapping("/registered-models/list")
    public Map<String, Object> listRegisteredModels() throws IOException {
        return Map.of("registered_models", modelRegistryService.listRegisteredModels());
    }

    @PostMapping("/model-versions/create")
    public Map<String, Object> createModelVersion(@RequestBody Map<String, String> request) throws IOException {
        String name = request.get("name");
        String source = request.get("source");
        String runId = request.get("run_id");
        ModelVersion version = modelRegistryService.createModelVersion(name, source, runId);
        return Map.of("model_version", version);
    }
    @GetMapping("/registered-models/search")
    public Map<String, Object> searchRegisteredModels(@RequestParam(value = "filter", required = false) String filter) throws IOException {
        return Map.of("registered_models", modelRegistryService.searchRegisteredModels(filter));
    }

    @PostMapping("/registered-models/update")
    public Map<String, Object> updateRegisteredModel(@RequestBody Map<String, String> request) throws IOException {
        String name = request.get("name");
        String description = request.get("description");
        modelRegistryService.updateRegisteredModel(name, description);
        return Map.of();
    }

    @PostMapping("/registered-models/delete")
    public Map<String, Object> deleteRegisteredModel(@RequestBody Map<String, String> request) throws IOException {
        String name = request.get("name");
        modelRegistryService.deleteRegisteredModel(name);
        return Map.of();
    }

    @GetMapping("/model-versions/get")
    public Map<String, Object> getModelVersion(@RequestParam("name") String name, @RequestParam("version") String version) throws IOException {
        return Map.of("model_version", modelRegistryService.getModelVersion(name, version));
    }

    @PostMapping("/model-versions/update")
    public Map<String, Object> updateModelVersion(@RequestBody Map<String, String> request) throws IOException {
        String name = request.get("name");
        String version = request.get("version");
        String description = request.get("description");
        modelRegistryService.updateModelVersion(name, version, description);
        return Map.of();
    }

    @PostMapping("/model-versions/delete")
    public Map<String, Object> deleteModelVersion(@RequestBody Map<String, String> request) throws IOException {
        String name = request.get("name");
        String version = request.get("version");
        modelRegistryService.deleteModelVersion(name, version);
        return Map.of();
    }

    @PostMapping("/model-versions/transition-stage")
    public Map<String, Object> transitionModelVersionStage(@RequestBody Map<String, String> request) throws IOException {
        String name = request.get("name");
        String version = request.get("version");
        String stage = request.get("stage");
        String archiveExistingVersions = request.get("archive_existing_versions");
        return Map.of("model_version", modelRegistryService.transitionModelVersionStage(name, version, stage, Boolean.parseBoolean(archiveExistingVersions)));
    }
    @PostMapping("/registered-models/set-tag")
    public Map<String, Object> setRegisteredModelTag(@RequestBody Map<String, String> request) throws IOException {
        String name = request.get("name");
        String key = request.get("key");
        String value = request.get("value");
        modelRegistryService.setRegisteredModelTag(name, key, value);
        return Map.of();
    }

    @PostMapping("/registered-models/delete-tag")
    public Map<String, Object> deleteRegisteredModelTag(@RequestBody Map<String, String> request) throws IOException {
        String name = request.get("name");
        String key = request.get("key");
        modelRegistryService.deleteRegisteredModelTag(name, key);
        return Map.of();
    }

    @PostMapping("/model-versions/set-tag")
    public Map<String, Object> setModelVersionTag(@RequestBody Map<String, String> request) throws IOException {
        String name = request.get("name");
        String version = request.get("version");
        String key = request.get("key");
        String value = request.get("value");
        modelRegistryService.setModelVersionTag(name, version, key, value);
        return Map.of();
    }

    @PostMapping("/model-versions/delete-tag")
    public Map<String, Object> deleteModelVersionTag(@RequestBody Map<String, String> request) throws IOException {
        String name = request.get("name");
        String version = request.get("version");
        String key = request.get("key");
        modelRegistryService.deleteModelVersionTag(name, version, key);
        return Map.of();
    }
}
