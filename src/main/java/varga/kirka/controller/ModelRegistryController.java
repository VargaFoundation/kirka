package varga.kirka.controller;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import varga.kirka.model.*;
import varga.kirka.service.ModelRegistryService;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;
import java.util.List;

@Slf4j
@RestController
@RequestMapping("/api/2.0/mlflow")
@RequiredArgsConstructor
public class ModelRegistryController {

    private final ModelRegistryService modelRegistryService;

    @lombok.Data
    public static class CreateRegisteredModelRequest {
        private String name;
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    public static class RegisteredModelResponse {
        private RegisteredModel registered_model;
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    public static class RegisteredModelsResponse {
        private List<RegisteredModel> registered_models;
    }

    @lombok.Data
    public static class CreateModelVersionRequest {
        private String name;
        private String source;
        private String run_id;
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    public static class ModelVersionResponse {
        private ModelVersion model_version;
    }

    @lombok.Data
    public static class UpdateRegisteredModelRequest {
        private String name;
        private String description;
    }

    @lombok.Data
    public static class DeleteRegisteredModelRequest {
        private String name;
    }

    @lombok.Data
    public static class UpdateModelVersionRequest {
        private String name;
        private String version;
        private String description;
    }

    @lombok.Data
    public static class DeleteModelVersionRequest {
        private String name;
        private String version;
    }

    @lombok.Data
    public static class TransitionModelVersionStageRequest {
        private String name;
        private String version;
        private String stage;
        private String archive_existing_versions;
    }

    @lombok.Data
    public static class SetRegisteredModelTagRequest {
        private String name;
        private String key;
        private String value;
    }

    @lombok.Data
    public static class DeleteRegisteredModelTagRequest {
        private String name;
        private String key;
    }

    @lombok.Data
    public static class SetModelVersionTagRequest {
        private String name;
        private String version;
        private String key;
        private String value;
    }

    @lombok.Data
    public static class DeleteModelVersionTagRequest {
        private String name;
        private String version;
        private String key;
    }

    @PostMapping("/registered-models/create")
    public RegisteredModelResponse createRegisteredModel(@RequestBody CreateRegisteredModelRequest request) throws IOException {
        String name = request.getName();
        log.info("REST request to create registered model: {}", name);
        modelRegistryService.createRegisteredModel(name);
        return new RegisteredModelResponse(modelRegistryService.getRegisteredModel(name));
    }

    @GetMapping("/registered-models/get")
    public RegisteredModelResponse getRegisteredModel(@RequestParam("name") String name) throws IOException {
        return new RegisteredModelResponse(modelRegistryService.getRegisteredModel(name));
    }

    @GetMapping("/registered-models/list")
    public RegisteredModelsResponse listRegisteredModels() throws IOException {
        return new RegisteredModelsResponse(modelRegistryService.listRegisteredModels());
    }

    @PostMapping("/model-versions/create")
    public ModelVersionResponse createModelVersion(@RequestBody CreateModelVersionRequest request) throws IOException {
        String name = request.getName();
        String source = request.getSource();
        String runId = request.getRun_id();
        ModelVersion version = modelRegistryService.createModelVersion(name, source, runId);
        return new ModelVersionResponse(version);
    }

    @GetMapping("/registered-models/search")
    public RegisteredModelsResponse searchRegisteredModels(@RequestParam(value = "filter", required = false) String filter) throws IOException {
        return new RegisteredModelsResponse(modelRegistryService.searchRegisteredModels(filter));
    }

    @PostMapping("/registered-models/update")
    public Map<String, Object> updateRegisteredModel(@RequestBody UpdateRegisteredModelRequest request) throws IOException {
        String name = request.getName();
        String description = request.getDescription();
        modelRegistryService.updateRegisteredModel(name, description);
        return Map.of();
    }

    @PostMapping("/registered-models/delete")
    public Map<String, Object> deleteRegisteredModel(@RequestBody DeleteRegisteredModelRequest request) throws IOException {
        String name = request.getName();
        modelRegistryService.deleteRegisteredModel(name);
        return Map.of();
    }

    @GetMapping("/model-versions/get")
    public ModelVersionResponse getModelVersion(@RequestParam("name") String name, @RequestParam("version") String version) throws IOException {
        return new ModelVersionResponse(modelRegistryService.getModelVersion(name, version));
    }

    @PostMapping("/model-versions/update")
    public Map<String, Object> updateModelVersion(@RequestBody UpdateModelVersionRequest request) throws IOException {
        String name = request.getName();
        String version = request.getVersion();
        String description = request.getDescription();
        modelRegistryService.updateModelVersion(name, version, description);
        return Map.of();
    }

    @PostMapping("/model-versions/delete")
    public Map<String, Object> deleteModelVersion(@RequestBody DeleteModelVersionRequest request) throws IOException {
        String name = request.getName();
        String version = request.getVersion();
        modelRegistryService.deleteModelVersion(name, version);
        return Map.of();
    }

    @PostMapping("/model-versions/transition-stage")
    public ModelVersionResponse transitionModelVersionStage(@RequestBody TransitionModelVersionStageRequest request) throws IOException {
        String name = request.getName();
        String version = request.getVersion();
        String stage = request.getStage();
        String archiveExistingVersions = request.getArchive_existing_versions();
        return new ModelVersionResponse(modelRegistryService.transitionModelVersionStage(name, version, stage, Boolean.parseBoolean(archiveExistingVersions)));
    }

    @PostMapping("/registered-models/set-tag")
    public Map<String, Object> setRegisteredModelTag(@RequestBody SetRegisteredModelTagRequest request) throws IOException {
        String name = request.getName();
        String key = request.getKey();
        String value = request.getValue();
        modelRegistryService.setRegisteredModelTag(name, key, value);
        return Map.of();
    }

    @PostMapping("/registered-models/delete-tag")
    public Map<String, Object> deleteRegisteredModelTag(@RequestBody DeleteRegisteredModelTagRequest request) throws IOException {
        String name = request.getName();
        String key = request.getKey();
        modelRegistryService.deleteRegisteredModelTag(name, key);
        return Map.of();
    }

    @PostMapping("/model-versions/set-tag")
    public Map<String, Object> setModelVersionTag(@RequestBody SetModelVersionTagRequest request) throws IOException {
        String name = request.getName();
        String version = request.getVersion();
        String key = request.getKey();
        String value = request.getValue();
        modelRegistryService.setModelVersionTag(name, version, key, value);
        return Map.of();
    }

    @PostMapping("/model-versions/delete-tag")
    public Map<String, Object> deleteModelVersionTag(@RequestBody DeleteModelVersionTagRequest request) throws IOException {
        String name = request.getName();
        String version = request.getVersion();
        String key = request.getKey();
        modelRegistryService.deleteModelVersionTag(name, version, key);
        return Map.of();
    }
}
