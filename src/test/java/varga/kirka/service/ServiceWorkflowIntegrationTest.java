package varga.kirka.service;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import varga.kirka.model.*;
import varga.kirka.repo.AbstractHBaseIntegrationTest;
import varga.kirka.repo.ArtifactRepository;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end service-level integration tests that verify cross-cutting workflows
 * across experiment, run, artifact, model registry, prompt, scorer, and gateway services.
 * All services interact with a real HBase mini-cluster.
 */
@SpringBootTest(properties = {
    "spring.main.allow-bean-definition-overriding=true",
    "security.kerberos.enabled=false"
})
@Import(AbstractHBaseIntegrationTest.HBaseTestConfig.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ServiceWorkflowIntegrationTest extends AbstractHBaseIntegrationTest {

    @Autowired
    private ExperimentService experimentService;

    @Autowired
    private RunService runService;

    @Autowired
    private ArtifactService artifactService;

    @Autowired
    private ModelRegistryService modelRegistryService;

    @Autowired
    private PromptService promptService;

    @Autowired
    private ScorerService scorerService;

    @Autowired
    private GatewayRouteService gatewayRouteService;

    @Autowired
    private GatewaySecretService gatewaySecretService;

    @Autowired
    private GatewayEndpointService gatewayEndpointService;

    @Autowired
    private ArtifactRepository artifactRepository;

    // =========================================================================
    // Full ML Lifecycle: Experiment → Run → Artifacts → Model Registry
    // =========================================================================

    @Test
    @Order(1)
    public void testFullMLLifecycle() throws IOException {
        // --- Step 1: Create an experiment ---
        String experimentId = experimentService.createExperiment(
                "ml-lifecycle-experiment",
                "hdfs:///mlflow/artifacts/lifecycle",
                List.of(new ExperimentTag("team", "ml-platform"))
        );
        assertNotNull(experimentId);

        Experiment experiment = experimentService.getExperiment(experimentId);
        assertEquals("ml-lifecycle-experiment", experiment.getName());
        assertEquals("active", experiment.getLifecycleStage());
        assertNotNull(experiment.getTags());
        assertTrue(experiment.getTags().stream()
                .anyMatch(t -> "team".equals(t.getKey()) && "ml-platform".equals(t.getValue())));

        // --- Step 2: Create a run within the experiment ---
        Run run = runService.createRun(experimentId, "test-user",
                System.currentTimeMillis(), Map.of("mlflow.runName", "training-run-1"));
        String runId = run.getInfo().getRunId();
        assertNotNull(runId);
        assertEquals(experimentId, run.getInfo().getExperimentId());
        assertEquals(RunStatus.RUNNING, run.getInfo().getStatus());

        // --- Step 3: Log parameters and metrics ---
        runService.logParameter(runId, "learning_rate", "0.001");
        runService.logParameter(runId, "batch_size", "32");
        runService.logMetric(runId, "accuracy", 0.85, System.currentTimeMillis(), 1);
        runService.logMetric(runId, "accuracy", 0.92, System.currentTimeMillis(), 2);
        runService.logMetric(runId, "loss", 0.15, System.currentTimeMillis(), 1);

        // Verify params and metrics were logged
        Run retrievedRun = runService.getRun(runId);
        assertNotNull(retrievedRun.getData());
        assertTrue(retrievedRun.getData().getParams().stream()
                .anyMatch(p -> "learning_rate".equals(p.getKey()) && "0.001".equals(p.getValue())));
        assertTrue(retrievedRun.getData().getParams().stream()
                .anyMatch(p -> "batch_size".equals(p.getKey()) && "32".equals(p.getValue())));
        assertTrue(retrievedRun.getData().getMetrics().stream()
                .anyMatch(m -> "accuracy".equals(m.getKey())));

        // --- Step 4: Upload artifacts for the run ---
        String artifactBasePath = run.getInfo().getArtifactUri();
        String modelArtifactPath = artifactBasePath + "/model/model.pkl";
        String configArtifactPath = artifactBasePath + "/model/config.json";

        byte[] modelContent = "serialized_model_weights_binary_data".getBytes(StandardCharsets.UTF_8);
        byte[] configContent = "{\"architecture\": \"transformer\", \"layers\": 12}".getBytes(StandardCharsets.UTF_8);

        artifactService.uploadArtifact(modelArtifactPath, new ByteArrayInputStream(modelContent));
        artifactService.uploadArtifact(configArtifactPath, new ByteArrayInputStream(configContent));

        // Verify artifacts exist
        assertTrue(artifactRepository.exists(modelArtifactPath));
        assertTrue(artifactRepository.exists(configArtifactPath));

        // Download and verify artifact content
        ByteArrayOutputStream downloadBuffer = new ByteArrayOutputStream();
        artifactService.downloadArtifact(configArtifactPath, downloadBuffer);
        assertEquals(new String(configContent, StandardCharsets.UTF_8), downloadBuffer.toString(StandardCharsets.UTF_8));

        // List artifacts in the model directory
        List<FileInfo> artifacts = artifactService.listArtifacts(artifactBasePath + "/model");
        assertNotNull(artifacts);
        assertEquals(2, artifacts.size());

        // --- Step 5: Complete the run ---
        runService.updateRun(runId, "FINISHED", System.currentTimeMillis());
        Run finishedRun = runService.getRun(runId);
        assertEquals(RunStatus.FINISHED, finishedRun.getInfo().getStatus());
        assertTrue(finishedRun.getInfo().getEndTime() > 0);

        // --- Step 6: Register the model ---
        modelRegistryService.createRegisteredModel("lifecycle-model");
        RegisteredModel model = modelRegistryService.getRegisteredModel("lifecycle-model");
        assertNotNull(model);
        assertEquals("lifecycle-model", model.getName());

        // --- Step 7: Create model version linked to the run ---
        ModelVersion version = modelRegistryService.createModelVersion(
                "lifecycle-model",
                artifactBasePath + "/model",
                runId
        );
        assertNotNull(version);
        assertEquals("1", version.getVersion());
        assertEquals(runId, version.getRunId());
        assertEquals(artifactBasePath + "/model", version.getSource());
        assertEquals(ModelVersionStatus.READY, version.getStatus());

        // Retrieve the version and verify linkage
        ModelVersion retrievedVersion = modelRegistryService.getModelVersion("lifecycle-model", "1");
        assertEquals(runId, retrievedVersion.getRunId());

        // --- Step 8: Transition model to Production ---
        ModelVersion prodVersion = modelRegistryService.transitionModelVersionStage(
                "lifecycle-model", "1", "Production", false);
        assertEquals("Production", prodVersion.getCurrentStage());

        // --- Step 9: Tag the model ---
        modelRegistryService.setRegisteredModelTag("lifecycle-model", "framework", "pytorch");
        RegisteredModel taggedModel = modelRegistryService.getRegisteredModel("lifecycle-model");
        assertNotNull(taggedModel.getTags());
        assertTrue(taggedModel.getTags().stream()
                .anyMatch(t -> "framework".equals(t.getKey()) && "pytorch".equals(t.getValue())));
    }

    // =========================================================================
    // Search Experiments and Runs
    // =========================================================================

    @Test
    @Order(2)
    public void testSearchExperimentsAndRuns() throws IOException {
        // Create multiple experiments
        String expId1 = experimentService.createExperiment("search-exp-1", null, null);
        String expId2 = experimentService.createExperiment("search-exp-2", null, null);

        // Create runs in both experiments
        Run run1 = runService.createRun(expId1, "user1", System.currentTimeMillis(), null);
        Run run2 = runService.createRun(expId1, "user2", System.currentTimeMillis(), null);
        Run run3 = runService.createRun(expId2, "user1", System.currentTimeMillis(), null);

        // Search experiments
        List<Experiment> experiments = experimentService.searchExperiments("ACTIVE_ONLY", 100, null);
        assertNotNull(experiments);
        assertTrue(experiments.stream().anyMatch(e -> "search-exp-1".equals(e.getName())));
        assertTrue(experiments.stream().anyMatch(e -> "search-exp-2".equals(e.getName())));

        // Search runs within a specific experiment
        List<Run> runsInExp1 = runService.searchRuns(List.of(expId1), null, "ACTIVE_ONLY");
        assertTrue(runsInExp1.size() >= 2);
        assertTrue(runsInExp1.stream().allMatch(r -> expId1.equals(r.getInfo().getExperimentId())));

        // Search runs across experiments
        List<Run> allRuns = runService.searchRuns(List.of(expId1, expId2), null, "ACTIVE_ONLY");
        assertTrue(allRuns.size() >= 3);
    }

    // =========================================================================
    // Experiment Delete/Restore Lifecycle
    // =========================================================================

    @Test
    @Order(3)
    public void testExperimentDeleteRestoreLifecycle() throws IOException {
        String expId = experimentService.createExperiment("delete-restore-exp", null, null);

        // Soft delete
        experimentService.deleteExperiment(expId);
        Experiment deleted = experimentService.getExperiment(expId);
        assertEquals("deleted", deleted.getLifecycleStage());

        // Should appear in DELETED_ONLY search
        List<Experiment> deletedExps = experimentService.searchExperiments("DELETED_ONLY", 100, null);
        assertTrue(deletedExps.stream().anyMatch(e -> expId.equals(e.getExperimentId())));

        // Restore
        experimentService.restoreExperiment(expId);
        Experiment restored = experimentService.getExperiment(expId);
        assertEquals("active", restored.getLifecycleStage());
    }

    // =========================================================================
    // Run Delete/Restore Lifecycle
    // =========================================================================

    @Test
    @Order(4)
    public void testRunDeleteRestoreLifecycle() throws IOException {
        String expId = experimentService.createExperiment("run-lifecycle-exp", null, null);
        Run run = runService.createRun(expId, "user1", System.currentTimeMillis(), null);
        String runId = run.getInfo().getRunId();

        // Delete run
        runService.deleteRun(runId);
        Run deletedRun = runService.getRun(runId);
        assertEquals("deleted", deletedRun.getInfo().getLifecycleStage());

        // Should not appear in ACTIVE_ONLY search
        List<Run> activeRuns = runService.searchRuns(List.of(expId), null, "ACTIVE_ONLY");
        assertTrue(activeRuns.stream().noneMatch(r -> runId.equals(r.getInfo().getRunId())));

        // Restore
        runService.restoreRun(runId);
        Run restoredRun = runService.getRun(runId);
        assertEquals("active", restoredRun.getInfo().getLifecycleStage());
    }

    // =========================================================================
    // Run Tags CRUD
    // =========================================================================

    @Test
    @Order(5)
    public void testRunTagsCRUD() throws IOException {
        String expId = experimentService.createExperiment("tag-test-exp", null, null);
        Run run = runService.createRun(expId, "user1", System.currentTimeMillis(), null);
        String runId = run.getInfo().getRunId();

        // Set tag
        runService.setTag(runId, "environment", "staging");
        Run tagged = runService.getRun(runId);
        assertTrue(tagged.getData().getTags().stream()
                .anyMatch(t -> "environment".equals(t.getKey()) && "staging".equals(t.getValue())));

        // Delete tag
        runService.deleteTag(runId, "environment");
        Run untagged = runService.getRun(runId);
        assertTrue(untagged.getData().getTags().stream()
                .noneMatch(t -> "environment".equals(t.getKey())));
    }

    // =========================================================================
    // Metric History
    // =========================================================================

    @Test
    @Order(6)
    public void testMetricHistory() throws IOException {
        String expId = experimentService.createExperiment("metric-history-exp", null, null);
        Run run = runService.createRun(expId, "user1", System.currentTimeMillis(), null);
        String runId = run.getInfo().getRunId();

        // Log multiple metric steps
        long baseTime = System.currentTimeMillis();
        runService.logMetric(runId, "val_loss", 1.5, baseTime, 0);
        runService.logMetric(runId, "val_loss", 1.2, baseTime + 1000, 1);
        runService.logMetric(runId, "val_loss", 0.8, baseTime + 2000, 2);
        runService.logMetric(runId, "val_loss", 0.5, baseTime + 3000, 3);

        List<Metric> history = runService.getMetricHistory(runId, "val_loss");
        assertNotNull(history);
        assertTrue(history.size() >= 4);
    }

    // =========================================================================
    // Model Registry Full Lifecycle
    // =========================================================================

    @Test
    @Order(7)
    public void testModelRegistryFullLifecycle() throws IOException {
        // Create model
        modelRegistryService.createRegisteredModel("registry-lifecycle-model");

        // Update description
        modelRegistryService.updateRegisteredModel("registry-lifecycle-model", "A test model for lifecycle testing");
        RegisteredModel updated = modelRegistryService.getRegisteredModel("registry-lifecycle-model");
        assertEquals("A test model for lifecycle testing", updated.getDescription());

        // Create first version
        ModelVersion v1 = modelRegistryService.createModelVersion(
                "registry-lifecycle-model", "hdfs:///models/v1", "run-1");
        assertEquals("1", v1.getVersion());
        assertEquals("None", v1.getCurrentStage());

        // Transition v1 to Staging
        modelRegistryService.transitionModelVersionStage(
                "registry-lifecycle-model", "1", "Staging", false);

        // Create second version
        ModelVersion v2 = modelRegistryService.createModelVersion(
                "registry-lifecycle-model", "hdfs:///models/v2", "run-2");
        assertEquals("2", v2.getVersion());

        // Transition v2 to Staging with archiveExistingVersions=true
        modelRegistryService.transitionModelVersionStage(
                "registry-lifecycle-model", "2", "Staging", true);

        // v1 should now be archived
        ModelVersion v1After = modelRegistryService.getModelVersion("registry-lifecycle-model", "1");
        assertEquals("Archived", v1After.getCurrentStage());

        // v2 should be in Staging
        ModelVersion v2After = modelRegistryService.getModelVersion("registry-lifecycle-model", "2");
        assertEquals("Staging", v2After.getCurrentStage());

        // Tag a model version
        modelRegistryService.setModelVersionTag("registry-lifecycle-model", "2", "approved", "true");

        // Search models
        List<RegisteredModel> models = modelRegistryService.searchRegisteredModels(null);
        assertTrue(models.stream().anyMatch(m -> "registry-lifecycle-model".equals(m.getName())));

        // Delete model version
        modelRegistryService.deleteModelVersion("registry-lifecycle-model", "1");
        assertThrows(ResourceNotFoundException.class,
                () -> modelRegistryService.getModelVersion("registry-lifecycle-model", "1"));

        // Delete model
        modelRegistryService.deleteRegisteredModel("registry-lifecycle-model");
        assertThrows(ResourceNotFoundException.class,
                () -> modelRegistryService.getRegisteredModel("registry-lifecycle-model"));
    }

    // =========================================================================
    // Multiple Model Versions Linked to Different Runs
    // =========================================================================

    @Test
    @Order(8)
    public void testMultipleVersionsLinkedToRuns() throws IOException {
        String expId = experimentService.createExperiment("multi-version-exp", null, null);

        // Create 3 runs with different hyperparameters
        Run run1 = runService.createRun(expId, "user1", System.currentTimeMillis(), null);
        runService.logParameter(run1.getInfo().getRunId(), "lr", "0.01");
        runService.logMetric(run1.getInfo().getRunId(), "accuracy", 0.80, System.currentTimeMillis(), 1);

        Run run2 = runService.createRun(expId, "user1", System.currentTimeMillis(), null);
        runService.logParameter(run2.getInfo().getRunId(), "lr", "0.001");
        runService.logMetric(run2.getInfo().getRunId(), "accuracy", 0.90, System.currentTimeMillis(), 1);

        Run run3 = runService.createRun(expId, "user1", System.currentTimeMillis(), null);
        runService.logParameter(run3.getInfo().getRunId(), "lr", "0.0001");
        runService.logMetric(run3.getInfo().getRunId(), "accuracy", 0.88, System.currentTimeMillis(), 1);

        // Register model and create versions for each run
        modelRegistryService.createRegisteredModel("multi-version-model");

        ModelVersion v1 = modelRegistryService.createModelVersion(
                "multi-version-model", "hdfs:///models/v1", run1.getInfo().getRunId());
        ModelVersion v2 = modelRegistryService.createModelVersion(
                "multi-version-model", "hdfs:///models/v2", run2.getInfo().getRunId());
        ModelVersion v3 = modelRegistryService.createModelVersion(
                "multi-version-model", "hdfs:///models/v3", run3.getInfo().getRunId());

        // Verify each version is linked to its run
        assertEquals(run1.getInfo().getRunId(),
                modelRegistryService.getModelVersion("multi-version-model", "1").getRunId());
        assertEquals(run2.getInfo().getRunId(),
                modelRegistryService.getModelVersion("multi-version-model", "2").getRunId());
        assertEquals(run3.getInfo().getRunId(),
                modelRegistryService.getModelVersion("multi-version-model", "3").getRunId());
    }

    // =========================================================================
    // Artifact Upload, Download, List, and Delete
    // =========================================================================

    @Test
    @Order(9)
    public void testArtifactLifecycle() throws IOException {
        String expId = experimentService.createExperiment("artifact-lifecycle-exp", null, null);
        Run run = runService.createRun(expId, "user1", System.currentTimeMillis(), null);
        String basePath = run.getInfo().getArtifactUri();

        // Upload multiple artifacts
        artifactService.uploadArtifact(basePath + "/data/train.csv",
                new ByteArrayInputStream("col1,col2\n1,2\n3,4".getBytes(StandardCharsets.UTF_8)));
        artifactService.uploadArtifact(basePath + "/data/test.csv",
                new ByteArrayInputStream("col1,col2\n5,6".getBytes(StandardCharsets.UTF_8)));
        artifactService.uploadArtifact(basePath + "/model.pkl",
                new ByteArrayInputStream("model_bytes".getBytes(StandardCharsets.UTF_8)));

        // List artifacts at root
        List<FileInfo> rootArtifacts = artifactService.listArtifacts(basePath);
        assertNotNull(rootArtifacts);
        assertTrue(rootArtifacts.size() >= 2); // data dir + model.pkl

        // List data directory
        List<FileInfo> dataArtifacts = artifactService.listArtifacts(basePath + "/data");
        assertEquals(2, dataArtifacts.size());

        // Download specific artifact
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        artifactService.downloadArtifact(basePath + "/data/train.csv", out);
        assertEquals("col1,col2\n1,2\n3,4", out.toString(StandardCharsets.UTF_8));

        // Delete artifact
        artifactService.deleteArtifact(basePath + "/model.pkl");
        assertFalse(artifactRepository.exists(basePath + "/model.pkl"));
    }

    // =========================================================================
    // Prompt Workflow
    // =========================================================================

    @Test
    @Order(10)
    public void testPromptWorkflow() throws IOException {
        // Create prompt
        Prompt prompt = promptService.createPrompt(
                "greeting-prompt",
                "Hello {{name}}, welcome to {{service}}!",
                "A greeting template",
                Map.of("category", "greeting", "language", "en")
        );
        assertNotNull(prompt.getId());
        assertEquals("greeting-prompt", prompt.getName());
        assertEquals("1", prompt.getVersion());
        assertEquals("Hello {{name}}, welcome to {{service}}!", prompt.getTemplate());

        // Get prompt
        Prompt retrieved = promptService.getPrompt(prompt.getId());
        assertEquals(prompt.getName(), retrieved.getName());
        assertEquals(prompt.getTemplate(), retrieved.getTemplate());
        assertEquals("A greeting template", retrieved.getDescription());
        assertNotNull(retrieved.getTags());
        assertEquals("greeting", retrieved.getTags().get("category"));

        // List prompts
        List<Prompt> prompts = promptService.listPrompts();
        assertTrue(prompts.stream().anyMatch(p -> prompt.getId().equals(p.getId())));

        // Delete prompt
        promptService.deletePrompt(prompt.getId());
        assertThrows(ResourceNotFoundException.class, () -> promptService.getPrompt(prompt.getId()));
    }

    // =========================================================================
    // Scorer Workflow
    // =========================================================================

    @Test
    @Order(11)
    public void testScorerWorkflow() throws IOException {
        String expId = experimentService.createExperiment("scorer-workflow-exp", null, null);

        // Register scorers
        Scorer s1v1 = scorerService.registerScorer(expId, "accuracy_scorer",
                "{\"type\":\"accuracy\",\"threshold\":0.9}");
        assertNotNull(s1v1.getScorerId());
        assertEquals("accuracy_scorer", s1v1.getScorerName());
        assertEquals(1, s1v1.getScorerVersion());

        // Register new version of same scorer
        Scorer s1v2 = scorerService.registerScorer(expId, "accuracy_scorer",
                "{\"type\":\"accuracy\",\"threshold\":0.95}");
        assertEquals(2, s1v2.getScorerVersion());

        // Register a different scorer
        Scorer s2 = scorerService.registerScorer(expId, "f1_scorer",
                "{\"type\":\"f1\"}");
        assertEquals(1, s2.getScorerVersion());

        // List scorers (latest versions only)
        List<Scorer> scorers = scorerService.listScorers(expId);
        assertEquals(2, scorers.size());
        assertTrue(scorers.stream().anyMatch(s -> "accuracy_scorer".equals(s.getScorerName()) && s.getScorerVersion() == 2));
        assertTrue(scorers.stream().anyMatch(s -> "f1_scorer".equals(s.getScorerName()) && s.getScorerVersion() == 1));

        // List all versions of a scorer
        List<Scorer> versions = scorerService.listScorerVersions(expId, "accuracy_scorer");
        assertEquals(2, versions.size());

        // Get specific version
        Scorer specific = scorerService.getScorer(expId, "accuracy_scorer", 1);
        assertEquals("{\"type\":\"accuracy\",\"threshold\":0.9}", specific.getSerializedScorer());

        // Delete a specific version
        scorerService.deleteScorer(expId, "accuracy_scorer", 1);
        assertThrows(ResourceNotFoundException.class,
                () -> scorerService.getScorer(expId, "accuracy_scorer", 1));

        // Version 2 should still exist
        Scorer v2 = scorerService.getScorer(expId, "accuracy_scorer", 2);
        assertEquals(2, v2.getScorerVersion());
    }

    // =========================================================================
    // Gateway Route Workflow
    // =========================================================================

    @Test
    @Order(12)
    public void testGatewayRouteWorkflow() throws IOException {
        // Create route
        GatewayRoute route = GatewayRoute.builder()
                .name("completions-route")
                .routeType("llm/v1/completions")
                .modelName("gpt-4")
                .modelProvider("openai")
                .config(Map.of("max_tokens", 2048))
                .build();
        GatewayRoute created = gatewayRouteService.createRoute(route);
        assertNotNull(created);
        assertEquals("completions-route", created.getName());

        // Get route
        GatewayRoute retrieved = gatewayRouteService.getRoute("completions-route");
        assertEquals("llm/v1/completions", retrieved.getRouteType());
        assertEquals("gpt-4", retrieved.getModelName());

        // Update route
        GatewayRoute updated = gatewayRouteService.updateRoute("completions-route",
                Map.of("model_name", "gpt-4-turbo", "model_provider", "openai"));
        assertEquals("gpt-4-turbo", updated.getModelName());

        // List routes
        List<GatewayRoute> routes = gatewayRouteService.listRoutes();
        assertTrue(routes.stream().anyMatch(r -> "completions-route".equals(r.getName())));

        // Delete route
        gatewayRouteService.deleteRoute("completions-route");
        assertThrows(ResourceNotFoundException.class,
                () -> gatewayRouteService.getRoute("completions-route"));
    }

    // =========================================================================
    // Gateway Secret Workflow
    // =========================================================================

    @Test
    @Order(13)
    public void testGatewaySecretWorkflow() {
        // Create secret
        GatewaySecretInfo secret = gatewaySecretService.createSecret(
                "openai-api-key",
                List.of(SecretValueEntry.builder().key("api_key").value("sk-test-123").build()),
                "openai",
                List.of(AuthConfigEntry.builder().key("header").value("Authorization").build()),
                "admin"
        );
        assertNotNull(secret.getSecretId());
        assertEquals("openai-api-key", secret.getSecretName());
        assertEquals("openai", secret.getProvider());
        // Values should be masked
        assertNotNull(secret.getMaskedValues());
        assertEquals(1, secret.getMaskedValues().size());
        assertEquals("********", secret.getMaskedValues().get(0).getValue());

        // Get secret by ID
        GatewaySecretInfo byId = gatewaySecretService.getSecret(secret.getSecretId(), null);
        assertEquals(secret.getSecretId(), byId.getSecretId());

        // Get secret by name
        GatewaySecretInfo byName = gatewaySecretService.getSecret(null, "openai-api-key");
        assertEquals(secret.getSecretId(), byName.getSecretId());

        // Update secret
        GatewaySecretInfo updated = gatewaySecretService.updateSecret(
                secret.getSecretId(),
                List.of(SecretValueEntry.builder().key("api_key").value("sk-new-456").build()),
                null,
                "admin"
        );
        assertNotNull(updated);
        assertEquals(secret.getSecretId(), updated.getSecretId());

        // List secrets
        List<GatewaySecretInfo> all = gatewaySecretService.listSecrets(null);
        assertTrue(all.stream().anyMatch(s -> secret.getSecretId().equals(s.getSecretId())));

        // List by provider
        List<GatewaySecretInfo> openai = gatewaySecretService.listSecrets("openai");
        assertTrue(openai.stream().anyMatch(s -> secret.getSecretId().equals(s.getSecretId())));

        // Delete
        gatewaySecretService.deleteSecret(secret.getSecretId());
        assertThrows(ResourceNotFoundException.class,
                () -> gatewaySecretService.getSecret(secret.getSecretId(), null));
    }

    // =========================================================================
    // Gateway Endpoint Workflow
    // =========================================================================

    @Test
    @Order(14)
    public void testGatewayEndpointWorkflow() {
        // Attach a model (auto-creates endpoint)
        GatewayEndpointModelConfig config = GatewayEndpointModelConfig.builder()
                .modelDefinitionId("model-def-1")
                .linkageType(GatewayModelLinkageType.PRIMARY)
                .weight(1.0f)
                .fallbackOrder(0)
                .build();

        GatewayEndpointModelMapping mapping = gatewayEndpointService.attachModel(
                "endpoint-workflow", config, "admin");
        assertNotNull(mapping.getMappingId());
        assertEquals("endpoint-workflow", mapping.getEndpointId());
        assertEquals("model-def-1", mapping.getModelDefinitionId());

        // Get endpoint
        GatewayEndpoint endpoint = gatewayEndpointService.getEndpoint("endpoint-workflow");
        assertNotNull(endpoint);
        assertEquals(1, endpoint.getModelMappings().size());

        // Attach a second model (fallback)
        GatewayEndpointModelConfig fallbackConfig = GatewayEndpointModelConfig.builder()
                .modelDefinitionId("model-def-2")
                .linkageType(GatewayModelLinkageType.FALLBACK)
                .weight(0.0f)
                .fallbackOrder(1)
                .build();
        GatewayEndpointModelMapping fallbackMapping = gatewayEndpointService.attachModel(
                "endpoint-workflow", fallbackConfig, "admin");
        assertNotNull(fallbackMapping);

        GatewayEndpoint withFallback = gatewayEndpointService.getEndpoint("endpoint-workflow");
        assertEquals(2, withFallback.getModelMappings().size());

        // Set tag
        gatewayEndpointService.setTag("endpoint-workflow",
                GatewayEndpointTag.builder().key("env").value("production").build());
        GatewayEndpoint tagged = gatewayEndpointService.getEndpoint("endpoint-workflow");
        assertTrue(tagged.getTags().stream()
                .anyMatch(t -> "env".equals(t.getKey()) && "production".equals(t.getValue())));

        // Overwrite tag
        gatewayEndpointService.setTag("endpoint-workflow",
                GatewayEndpointTag.builder().key("env").value("staging").build());
        GatewayEndpoint retagged = gatewayEndpointService.getEndpoint("endpoint-workflow");
        assertEquals(1, retagged.getTags().stream().filter(t -> "env".equals(t.getKey())).count());
        assertTrue(retagged.getTags().stream()
                .anyMatch(t -> "env".equals(t.getKey()) && "staging".equals(t.getValue())));

        // Delete tag
        gatewayEndpointService.deleteTag("endpoint-workflow", "env");
        GatewayEndpoint untagged = gatewayEndpointService.getEndpoint("endpoint-workflow");
        assertTrue(untagged.getTags().stream().noneMatch(t -> "env".equals(t.getKey())));

        // Detach model
        gatewayEndpointService.detachModel("endpoint-workflow", mapping.getMappingId());
        GatewayEndpoint afterDetach = gatewayEndpointService.getEndpoint("endpoint-workflow");
        assertEquals(1, afterDetach.getModelMappings().size());
        assertEquals(fallbackMapping.getMappingId(), afterDetach.getModelMappings().get(0).getMappingId());

        // List endpoints
        List<GatewayEndpoint> endpoints = gatewayEndpointService.listEndpoints();
        assertTrue(endpoints.stream().anyMatch(e -> "endpoint-workflow".equals(e.getEndpointId())));
    }

    // =========================================================================
    // Error Cases: ResourceNotFoundException
    // =========================================================================

    @Test
    @Order(15)
    public void testResourceNotFoundErrors() {
        assertThrows(ResourceNotFoundException.class,
                () -> experimentService.getExperiment("nonexistent-exp-id"));

        assertThrows(ResourceNotFoundException.class,
                () -> runService.getRun("nonexistent-run-id"));

        assertThrows(ResourceNotFoundException.class,
                () -> modelRegistryService.getRegisteredModel("nonexistent-model"));

        assertThrows(ResourceNotFoundException.class,
                () -> promptService.getPrompt("nonexistent-prompt-id"));

        assertThrows(ResourceNotFoundException.class,
                () -> scorerService.getScorer("nonexistent-exp", "nonexistent-scorer", 1));

        assertThrows(ResourceNotFoundException.class,
                () -> gatewayRouteService.getRoute("nonexistent-route"));

        assertThrows(ResourceNotFoundException.class,
                () -> gatewaySecretService.getSecret("nonexistent-secret-id", null));

        assertThrows(ResourceNotFoundException.class,
                () -> gatewayEndpointService.getEndpoint("nonexistent-endpoint"));
    }

    // =========================================================================
    // Validation Errors
    // =========================================================================

    @Test
    @Order(16)
    public void testValidationErrors() {
        assertThrows(IllegalArgumentException.class,
                () -> experimentService.createExperiment("", null, null));

        assertThrows(IllegalArgumentException.class,
                () -> experimentService.createExperiment(null, null, null));

        assertThrows(IllegalArgumentException.class,
                () -> runService.createRun("", "user", System.currentTimeMillis(), null));

        assertThrows(IllegalArgumentException.class,
                () -> modelRegistryService.createRegisteredModel(""));

        assertThrows(IllegalArgumentException.class,
                () -> promptService.createPrompt("", "template", null, null));

        assertThrows(IllegalArgumentException.class,
                () -> promptService.createPrompt("name", "", null, null));

        assertThrows(IllegalArgumentException.class,
                () -> scorerService.registerScorer("", "name", "{}"));

        assertThrows(IllegalArgumentException.class,
                () -> scorerService.registerScorer("exp", "", "{}"));

        assertThrows(IllegalArgumentException.class,
                () -> gatewaySecretService.createSecret("", null, null, null, null));
    }
}
