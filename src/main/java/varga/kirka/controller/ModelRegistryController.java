package varga.kirka.controller;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
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
        @NotBlank
        @Size(max = 256)
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
        private String next_page_token;

        public RegisteredModelsResponse(List<RegisteredModel> registered_models) {
            this(registered_models, null);
        }
    }

    @lombok.Data
    public static class CreateModelVersionRequest {
        @NotBlank private String name;
        @NotBlank @Size(max = 1024) private String source;
        private String run_id;
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    public static class ModelVersionResponse {
        private ModelVersion model_version;
    }

    @lombok.Data
    public static class UpdateRegisteredModelRequest {
        @NotBlank private String name;
        @Size(max = 5000) private String description;
    }

    @lombok.Data
    public static class DeleteRegisteredModelRequest {
        @NotBlank private String name;
    }

    @lombok.Data
    public static class UpdateModelVersionRequest {
        @NotBlank private String name;
        @NotBlank private String version;
        @Size(max = 5000) private String description;
    }

    @lombok.Data
    public static class DeleteModelVersionRequest {
        @NotBlank private String name;
        @NotBlank private String version;
    }

    @lombok.Data
    public static class TransitionModelVersionStageRequest {
        @NotBlank private String name;
        @NotBlank private String version;
        @NotBlank private String stage;
        private String archive_existing_versions;
    }

    @lombok.Data
    public static class SetRegisteredModelTagRequest {
        @NotBlank private String name;
        @NotBlank @Size(max = 250) private String key;
        @Size(max = 5000) private String value;
    }

    @lombok.Data
    public static class DeleteRegisteredModelTagRequest {
        @NotBlank private String name;
        @NotBlank @Size(max = 250) private String key;
    }

    @lombok.Data
    public static class SetModelVersionTagRequest {
        @NotBlank private String name;
        @NotBlank private String version;
        @NotBlank @Size(max = 250) private String key;
        @Size(max = 5000) private String value;
    }

    @lombok.Data
    public static class DeleteModelVersionTagRequest {
        @NotBlank private String name;
        @NotBlank private String version;
        @NotBlank @Size(max = 250) private String key;
    }

    @PostMapping("/registered-models/create")
    public RegisteredModelResponse createRegisteredModel(@Valid @RequestBody CreateRegisteredModelRequest request) throws IOException {
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
    public RegisteredModelsResponse listRegisteredModels(
            @RequestParam(value = "max_results", required = false) Integer maxResults,
            @RequestParam(value = "page_token", required = false) String pageToken) throws IOException {
        if (maxResults == null && pageToken == null) {
            return new RegisteredModelsResponse(modelRegistryService.listRegisteredModels());
        }
        var page = modelRegistryService.listRegisteredModelsPaged(maxResults, pageToken);
        return new RegisteredModelsResponse(page.items(), page.nextPageToken());
    }

    @PostMapping("/model-versions/create")
    public ModelVersionResponse createModelVersion(@Valid @RequestBody CreateModelVersionRequest request) throws IOException {
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
    public Map<String, Object> updateRegisteredModel(@Valid @RequestBody UpdateRegisteredModelRequest request) throws IOException {
        String name = request.getName();
        String description = request.getDescription();
        modelRegistryService.updateRegisteredModel(name, description);
        return Map.of();
    }

    @PostMapping("/registered-models/delete")
    public Map<String, Object> deleteRegisteredModel(@Valid @RequestBody DeleteRegisteredModelRequest request) throws IOException {
        String name = request.getName();
        modelRegistryService.deleteRegisteredModel(name);
        return Map.of();
    }

    @GetMapping("/model-versions/get")
    public ModelVersionResponse getModelVersion(@RequestParam("name") String name, @RequestParam("version") String version) throws IOException {
        return new ModelVersionResponse(modelRegistryService.getModelVersion(name, version));
    }

    @PostMapping("/model-versions/update")
    public Map<String, Object> updateModelVersion(@Valid @RequestBody UpdateModelVersionRequest request) throws IOException {
        String name = request.getName();
        String version = request.getVersion();
        String description = request.getDescription();
        modelRegistryService.updateModelVersion(name, version, description);
        return Map.of();
    }

    @PostMapping("/model-versions/delete")
    public Map<String, Object> deleteModelVersion(@Valid @RequestBody DeleteModelVersionRequest request) throws IOException {
        String name = request.getName();
        String version = request.getVersion();
        modelRegistryService.deleteModelVersion(name, version);
        return Map.of();
    }

    @PostMapping("/model-versions/transition-stage")
    public ModelVersionResponse transitionModelVersionStage(@Valid @RequestBody TransitionModelVersionStageRequest request) throws IOException {
        String name = request.getName();
        String version = request.getVersion();
        String stage = request.getStage();
        String archiveExistingVersions = request.getArchive_existing_versions();
        return new ModelVersionResponse(modelRegistryService.transitionModelVersionStage(name, version, stage, Boolean.parseBoolean(archiveExistingVersions)));
    }

    @PostMapping("/registered-models/set-tag")
    public Map<String, Object> setRegisteredModelTag(@Valid @RequestBody SetRegisteredModelTagRequest request) throws IOException {
        String name = request.getName();
        String key = request.getKey();
        String value = request.getValue();
        modelRegistryService.setRegisteredModelTag(name, key, value);
        return Map.of();
    }

    @PostMapping("/registered-models/delete-tag")
    public Map<String, Object> deleteRegisteredModelTag(@Valid @RequestBody DeleteRegisteredModelTagRequest request) throws IOException {
        String name = request.getName();
        String key = request.getKey();
        modelRegistryService.deleteRegisteredModelTag(name, key);
        return Map.of();
    }

    @PostMapping("/model-versions/set-tag")
    public Map<String, Object> setModelVersionTag(@Valid @RequestBody SetModelVersionTagRequest request) throws IOException {
        String name = request.getName();
        String version = request.getVersion();
        String key = request.getKey();
        String value = request.getValue();
        modelRegistryService.setModelVersionTag(name, version, key, value);
        return Map.of();
    }

    @PostMapping("/model-versions/delete-tag")
    public Map<String, Object> deleteModelVersionTag(@Valid @RequestBody DeleteModelVersionTagRequest request) throws IOException {
        String name = request.getName();
        String version = request.getVersion();
        String key = request.getKey();
        modelRegistryService.deleteModelVersionTag(name, version, key);
        return Map.of();
    }

    // ---- MLFlow 2.x extensions: rename, aliases, search model versions, download uri ----

    @lombok.Data
    public static class RenameRegisteredModelRequest {
        @NotBlank private String name;
        @NotBlank @Size(max = 256) private String new_name;
    }

    @PostMapping("/registered-models/rename")
    public RegisteredModelResponse renameRegisteredModel(@Valid @RequestBody RenameRegisteredModelRequest request) throws IOException {
        modelRegistryService.renameRegisteredModel(request.getName(), request.getNew_name());
        return new RegisteredModelResponse(modelRegistryService.getRegisteredModel(request.getNew_name()));
    }

    @lombok.Data
    public static class SetRegisteredModelAliasRequest {
        @NotBlank private String name;
        @NotBlank @Size(max = 256) private String alias;
        @NotBlank private String version;
    }

    @lombok.Data
    public static class DeleteRegisteredModelAliasRequest {
        @NotBlank private String name;
        @NotBlank @Size(max = 256) private String alias;
    }

    @PostMapping("/registered-models/alias")
    public Map<String, Object> setRegisteredModelAlias(@Valid @RequestBody SetRegisteredModelAliasRequest request) throws IOException {
        modelRegistryService.setAlias(request.getName(), request.getAlias(), request.getVersion());
        return Map.of();
    }

    @DeleteMapping("/registered-models/alias")
    public Map<String, Object> deleteRegisteredModelAlias(@RequestParam("name") String name,
                                                          @RequestParam("alias") String alias) throws IOException {
        modelRegistryService.deleteAlias(name, alias);
        return Map.of();
    }

    @GetMapping("/registered-models/alias")
    public ModelVersionResponse getModelVersionByAlias(@RequestParam("name") String name,
                                                       @RequestParam("alias") String alias) throws IOException {
        return new ModelVersionResponse(modelRegistryService.getModelVersionByAlias(name, alias));
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    public static class ModelVersionsResponse {
        private List<ModelVersion> model_versions;
    }

    @GetMapping("/model-versions/search")
    public ModelVersionsResponse searchModelVersions(@RequestParam(value = "filter", required = false) String filter,
                                                     @RequestParam(value = "max_results", required = false) Integer maxResults) throws IOException {
        int cap = maxResults != null ? maxResults : 1000;
        return new ModelVersionsResponse(modelRegistryService.searchModelVersions(filter, cap));
    }

    @lombok.Data
    @lombok.AllArgsConstructor
    public static class DownloadUriResponse {
        private String artifact_uri;
    }

    @GetMapping("/model-versions/get-download-uri")
    public DownloadUriResponse getModelVersionDownloadUri(@RequestParam("name") String name,
                                                          @RequestParam("version") String version) throws IOException {
        return new DownloadUriResponse(modelRegistryService.getModelVersionDownloadUri(name, version));
    }
}
