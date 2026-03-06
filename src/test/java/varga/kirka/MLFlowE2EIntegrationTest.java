package varga.kirka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.*;
import org.mlflow.api.proto.Service;
import org.mlflow.api.proto.Service.ViewType;
import org.mlflow.tracking.MlflowClient;
import org.mlflow.tracking.RunsPage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import varga.kirka.repo.AbstractHBaseIntegrationTest;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * End-to-end integration test that exercises the full MLflow-compatible API surface.
 * Uses the official MlflowClient (including sendPost/sendGet for model registry)
 * and TestRestTemplate for custom Kirka endpoints (prompts, scorers, gateway, artifacts).
 * All operations hit the live HTTP server backed by a real HBase mini-cluster.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.main.allow-bean-definition-overriding=true",
                "security.kerberos.enabled=false"
        }
)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MLFlowE2EIntegrationTest extends AbstractHBaseIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private Connection hbaseConnection;

    private MlflowClient mlflow() {
        return new MlflowClient("http://localhost:" + port);
    }

    private String api(String path) {
        return "/api/2.0/mlflow" + path;
    }

    // =========================================================================
    // 1. Full ML Lifecycle via MlflowClient
    //    Experiment → Run → Params/Metrics/Tags → Artifacts → Model Registry
    // =========================================================================

    @Test
    @Order(1)
    public void testFullMLLifecycleViaMlflowClient() throws Exception {
        MlflowClient client = mlflow();

        // --- Experiment creation ---
        String experimentName = "e2e_lifecycle_" + System.currentTimeMillis();
        String experimentId = client.createExperiment(experimentName);
        assertNotNull(experimentId);

        // Verify in HBase
        try (Table table = hbaseConnection.getTable(TableName.valueOf("mlflow_experiments"))) {
            Result result = table.get(new Get(Bytes.toBytes(experimentId)));
            assertFalse(result.isEmpty());
            assertEquals(experimentName,
                    Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
        }

        // --- Run creation + log params/metrics/tags ---
        Service.RunInfo runInfo = client.createRun(experimentId);
        String runId = runInfo.getRunId();
        assertNotNull(runId);
        assertFalse(runId.isBlank());

        client.logParam(runId, "learning_rate", "0.001");
        client.logParam(runId, "batch_size", "32");
        client.logMetric(runId, "accuracy", 0.85);
        client.logMetric(runId, "accuracy", 0.92);
        client.logMetric(runId, "loss", 0.15);
        client.setTag(runId, "mlflow.runName", "training-run-1");
        client.setTag(runId, "project", "kirka");

        Service.Run run = client.getRun(runId);
        assertEquals(runId, run.getInfo().getRunId());
        assertEquals(experimentId, run.getInfo().getExperimentId());
        assertTrue(run.getData().getParamsList().stream()
                .anyMatch(p -> "learning_rate".equals(p.getKey()) && "0.001".equals(p.getValue())));
        assertTrue(run.getData().getParamsList().stream()
                .anyMatch(p -> "batch_size".equals(p.getKey()) && "32".equals(p.getValue())));
        assertTrue(run.getData().getMetricsList().stream()
                .anyMatch(m -> "accuracy".equals(m.getKey())));
        assertTrue(run.getData().getTagsList().stream()
                .anyMatch(t -> "project".equals(t.getKey()) && "kirka".equals(t.getValue())));

        // --- Artifact upload via REST ---
        String artifactUri = run.getInfo().getArtifactUri();

        uploadArtifact(runId, "model", "model.pkl", "serialized_model_weights");
        uploadArtifact(runId, "model", "config.json", "{\"architecture\":\"transformer\",\"layers\":12}");

        // List artifacts via MlflowClient
        var artifacts = client.listArtifacts(runId);
        assertNotNull(artifacts);

        // Download artifact via REST
        byte[] downloaded = downloadArtifact(runId, "model/config.json");
        assertEquals("{\"architecture\":\"transformer\",\"layers\":12}", new String(downloaded));

        // --- Search runs ---
        RunsPage runsPage = client.searchRuns(List.of(experimentId), "", ViewType.ACTIVE_ONLY, 100);
        assertTrue(runsPage.getItems().size() >= 1);
        assertTrue(runsPage.getItems().stream()
                .anyMatch(r -> runId.equals(r.getInfo().getRunId())));

        // --- Register model via sendPost and create version linked to this run ---
        String modelName = experimentName + "-model";
        client.sendPost("registered-models/create",
                MAPPER.writeValueAsString(Map.of("name", modelName)));

        var model = client.getRegisteredModel(modelName);
        assertNotNull(model);
        assertEquals(modelName, model.getName());

        String versionJson = client.sendPost("model-versions/create",
                MAPPER.writeValueAsString(Map.of(
                        "name", modelName,
                        "source", artifactUri + "/model",
                        "run_id", runId)));
        JsonNode versionNode = MAPPER.readTree(versionJson).get("model_version");
        assertEquals("1", versionNode.get("version").asText());
        assertEquals(runId, versionNode.get("run_id").asText());

        // Transition to Production
        String stageJson = client.sendPost("model-versions/transition-stage",
                MAPPER.writeValueAsString(Map.of(
                        "name", modelName,
                        "version", "1",
                        "stage", "Production")));
        JsonNode stageNode = MAPPER.readTree(stageJson).get("model_version");
        assertEquals("Production", stageNode.get("current_stage").asText());

        // Tag model
        client.sendPost("registered-models/set-tag",
                MAPPER.writeValueAsString(Map.of(
                        "name", modelName, "key", "framework", "value", "pytorch")));
        var taggedModel = client.getRegisteredModel(modelName);
        assertTrue(taggedModel.getTagsList().stream()
                .anyMatch(t -> "framework".equals(t.getKey()) && "pytorch".equals(t.getValue())));
    }

    // =========================================================================
    // 2. Experiment Lifecycle via MlflowClient
    // =========================================================================

    @Test
    @Order(2)
    public void testExperimentLifecycleViaMlflowClient() throws Exception {
        MlflowClient client = mlflow();

        // Create
        String name = "exp_lifecycle_" + System.currentTimeMillis();
        String expId = client.createExperiment(name);
        assertNotNull(expId);

        // Get
        Service.Experiment exp = client.getExperiment(expId);
        assertEquals(name, exp.getName());
        assertEquals("active", exp.getLifecycleStage());

        // Rename
        String newName = name + "_renamed";
        client.renameExperiment(expId, newName);
        Service.Experiment renamed = client.getExperiment(expId);
        assertEquals(newName, renamed.getName());

        // Get by name
        var byName = client.getExperimentByName(newName);
        assertTrue(byName.isPresent());
        assertEquals(expId, byName.get().getExperimentId());

        // Set tag
        client.setExperimentTag(expId, "team", "ml-platform");
        Service.Experiment tagged = client.getExperiment(expId);
        assertTrue(tagged.getTagsList().stream()
                .anyMatch(t -> "team".equals(t.getKey()) && "ml-platform".equals(t.getValue())));

        // Delete (soft)
        client.deleteExperiment(expId);
        Service.Experiment deleted = client.getExperiment(expId);
        assertEquals("deleted", deleted.getLifecycleStage());

        // Restore
        client.restoreExperiment(expId);
        Service.Experiment restored = client.getExperiment(expId);
        assertEquals("active", restored.getLifecycleStage());
    }

    // =========================================================================
    // 3. Search Experiments and Runs across Multiple Experiments
    // =========================================================================

    @Test
    @Order(3)
    public void testSearchExperimentsAndRuns() throws Exception {
        MlflowClient client = mlflow();

        String expId1 = client.createExperiment("search_exp_1_" + System.currentTimeMillis());
        String expId2 = client.createExperiment("search_exp_2_" + System.currentTimeMillis());

        // Create runs in both
        client.createRun(expId1);
        client.createRun(expId1);
        client.createRun(expId2);

        // Search runs in exp1
        RunsPage page1 = client.searchRuns(List.of(expId1), "", ViewType.ACTIVE_ONLY, 100);
        assertTrue(page1.getItems().size() >= 2);
        assertTrue(page1.getItems().stream()
                .allMatch(r -> expId1.equals(r.getInfo().getExperimentId())));

        // Search across both
        RunsPage allPage = client.searchRuns(List.of(expId1, expId2), "", ViewType.ACTIVE_ONLY, 100);
        assertTrue(allPage.getItems().size() >= 3);

        // Search experiments
        var experimentsPage = client.searchExperiments();
        assertNotNull(experimentsPage);
        assertTrue(experimentsPage.getItems().size() >= 2);
    }

    // =========================================================================
    // 4. Run Lifecycle: Terminate, Delete/Restore, Tags CRUD, Metric History
    // =========================================================================

    @Test
    @Order(4)
    public void testRunLifecycleViaMlflowClient() throws Exception {
        MlflowClient client = mlflow();

        String expId = client.createExperiment("run_lifecycle_" + System.currentTimeMillis());
        Service.RunInfo runInfo = client.createRun(expId);
        String runId = runInfo.getRunId();

        // Log metrics at multiple steps
        client.logMetric(runId, "val_loss", 1.5, System.currentTimeMillis(), 0);
        client.logMetric(runId, "val_loss", 1.2, System.currentTimeMillis(), 1);
        client.logMetric(runId, "val_loss", 0.8, System.currentTimeMillis(), 2);

        // Get metric history
        var history = client.getMetricHistory(runId, "val_loss");
        assertNotNull(history);
        assertTrue(history.size() >= 3);

        // Terminate run
        client.setTerminated(runId, Service.RunStatus.FINISHED, System.currentTimeMillis());
        Service.Run terminated = client.getRun(runId);
        assertEquals(Service.RunStatus.FINISHED, terminated.getInfo().getStatus());
        assertTrue(terminated.getInfo().getEndTime() > 0);

        // Set and delete tag
        client.setTag(runId, "environment", "staging");
        Service.Run tagged = client.getRun(runId);
        assertTrue(tagged.getData().getTagsList().stream()
                .anyMatch(t -> "environment".equals(t.getKey()) && "staging".equals(t.getValue())));

        client.deleteTag(runId, "environment");
        Service.Run untagged = client.getRun(runId);
        assertTrue(untagged.getData().getTagsList().stream()
                .noneMatch(t -> "environment".equals(t.getKey())));

        // Delete run
        client.deleteRun(runId);
        RunsPage activeRuns = client.searchRuns(List.of(expId), "", ViewType.ACTIVE_ONLY, 100);
        assertTrue(activeRuns.getItems().stream()
                .noneMatch(r -> runId.equals(r.getInfo().getRunId())));

        // Restore run
        client.restoreRun(runId);
        RunsPage afterRestore = client.searchRuns(List.of(expId), "", ViewType.ACTIVE_ONLY, 100);
        assertTrue(afterRestore.getItems().stream()
                .anyMatch(r -> runId.equals(r.getInfo().getRunId())));
    }

    // =========================================================================
    // 5. Model Registry Full Lifecycle via MlflowClient sendPost/sendGet
    // =========================================================================

    @Test
    @Order(5)
    public void testModelRegistryLifecycleViaMlflowClient() throws Exception {
        MlflowClient client = mlflow();
        String modelName = "registry_lifecycle_" + System.currentTimeMillis();

        // Create
        client.sendPost("registered-models/create",
                MAPPER.writeValueAsString(Map.of("name", modelName)));
        var model = client.getRegisteredModel(modelName);
        assertEquals(modelName, model.getName());

        // Update description
        client.sendPost("registered-models/update",
                MAPPER.writeValueAsString(Map.of("name", modelName, "description", "A lifecycle test model")));
        var updated = client.getRegisteredModel(modelName);
        assertEquals("A lifecycle test model", updated.getDescription());

        // Create version 1, transition to Staging
        client.sendPost("model-versions/create",
                MAPPER.writeValueAsString(Map.of("name", modelName, "source", "hdfs:///models/v1", "run_id", "r1")));
        client.sendPost("model-versions/transition-stage",
                MAPPER.writeValueAsString(Map.of("name", modelName, "version", "1", "stage", "Staging")));

        // Create version 2
        client.sendPost("model-versions/create",
                MAPPER.writeValueAsString(Map.of("name", modelName, "source", "hdfs:///models/v2", "run_id", "r2")));

        // Transition v2 to Staging with archive
        client.sendPost("model-versions/transition-stage",
                MAPPER.writeValueAsString(Map.of(
                        "name", modelName, "version", "2",
                        "stage", "Staging", "archive_existing_versions", "true")));

        // v1 should be Archived
        var v1 = client.getModelVersion(modelName, "1");
        assertEquals("Archived", v1.getCurrentStage());

        // v2 should be Staging
        var v2 = client.getModelVersion(modelName, "2");
        assertEquals("Staging", v2.getCurrentStage());

        // Tag model and version
        client.sendPost("registered-models/set-tag",
                MAPPER.writeValueAsString(Map.of("name", modelName, "key", "team", "value", "ml-platform")));
        client.sendPost("model-versions/set-tag",
                MAPPER.writeValueAsString(Map.of("name", modelName, "version", "2", "key", "approved", "value", "true")));

        var taggedModel = client.getRegisteredModel(modelName);
        assertTrue(taggedModel.getTagsList().stream()
                .anyMatch(t -> "team".equals(t.getKey()) && "ml-platform".equals(t.getValue())));

        // Delete tags
        client.sendPost("registered-models/delete-tag",
                MAPPER.writeValueAsString(Map.of("name", modelName, "key", "team")));
        client.sendPost("model-versions/delete-tag",
                MAPPER.writeValueAsString(Map.of("name", modelName, "version", "2", "key", "approved")));

        // Delete version
        client.sendPost("model-versions/delete",
                MAPPER.writeValueAsString(Map.of("name", modelName, "version", "1")));

        // Delete model
        client.sendPost("registered-models/delete",
                MAPPER.writeValueAsString(Map.of("name", modelName)));
    }

    // =========================================================================
    // 6. Multiple Model Versions Linked to Different Runs
    // =========================================================================

    @Test
    @Order(6)
    public void testMultipleVersionsLinkedToRuns() throws Exception {
        MlflowClient client = mlflow();

        String expId = client.createExperiment("multi_version_" + System.currentTimeMillis());
        String modelName = "multi_version_model_" + System.currentTimeMillis();

        // Create 3 runs
        Service.RunInfo r1 = client.createRun(expId);
        client.logParam(r1.getRunId(), "lr", "0.01");
        client.logMetric(r1.getRunId(), "accuracy", 0.80);

        Service.RunInfo r2 = client.createRun(expId);
        client.logParam(r2.getRunId(), "lr", "0.001");
        client.logMetric(r2.getRunId(), "accuracy", 0.90);

        Service.RunInfo r3 = client.createRun(expId);
        client.logParam(r3.getRunId(), "lr", "0.0001");
        client.logMetric(r3.getRunId(), "accuracy", 0.88);

        // Register model + create versions linked to each run
        client.sendPost("registered-models/create",
                MAPPER.writeValueAsString(Map.of("name", modelName)));

        client.sendPost("model-versions/create",
                MAPPER.writeValueAsString(Map.of("name", modelName, "source", "hdfs:///v1", "run_id", r1.getRunId())));
        client.sendPost("model-versions/create",
                MAPPER.writeValueAsString(Map.of("name", modelName, "source", "hdfs:///v2", "run_id", r2.getRunId())));
        client.sendPost("model-versions/create",
                MAPPER.writeValueAsString(Map.of("name", modelName, "source", "hdfs:///v3", "run_id", r3.getRunId())));

        // Verify each version is linked to its run
        assertEquals(r1.getRunId(), client.getModelVersion(modelName, "1").getRunId());
        assertEquals(r2.getRunId(), client.getModelVersion(modelName, "2").getRunId());
        assertEquals(r3.getRunId(), client.getModelVersion(modelName, "3").getRunId());
    }

    // =========================================================================
    // 7. Artifact Upload, Download, List, Delete via REST
    // =========================================================================

    @Test
    @Order(7)
    public void testArtifactLifecycleViaREST() throws Exception {
        MlflowClient client = mlflow();

        String expId = client.createExperiment("artifact_lifecycle_" + System.currentTimeMillis());
        Service.RunInfo runInfo = client.createRun(expId);
        String runId = runInfo.getRunId();

        // Upload artifacts via REST
        uploadArtifact(runId, "data", "train.csv", "col1,col2\n1,2\n3,4");
        uploadArtifact(runId, "data", "test.csv", "col1,col2\n5,6");
        uploadArtifact(runId, null, "model.pkl", "model_binary_data");

        // List artifacts via MlflowClient
        var artifacts = client.listArtifacts(runId);
        assertNotNull(artifacts);
        assertTrue(artifacts.size() >= 2); // data/ dir + model.pkl

        // List sub-directory
        var dataArtifacts = client.listArtifacts(runId, "data");
        assertEquals(2, dataArtifacts.size());

        // Download via REST
        byte[] content = downloadArtifact(runId, "data/train.csv");
        assertEquals("col1,col2\n1,2\n3,4", new String(content));

        // Delete via REST
        restTemplate.postForEntity(api("/artifacts/delete"),
                Map.of("run_id", runId, "path", "model.pkl"), Map.class);

        // Verify deleted
        var afterDelete = client.listArtifacts(runId);
        assertTrue(afterDelete.stream()
                .noneMatch(f -> f.getPath().contains("model.pkl")));
    }

    // =========================================================================
    // 8. Prompt Workflow via REST
    // =========================================================================

    @Test
    @Order(8)
    @SuppressWarnings("unchecked")
    public void testPromptWorkflowViaREST() {
        // Create
        ResponseEntity<Map> createResp = restTemplate.postForEntity(
                api("/prompts/create"),
                Map.of("name", "greeting-prompt",
                        "template", "Hello {{name}}, welcome to {{service}}!",
                        "description", "A greeting template",
                        "tags", Map.of("category", "greeting", "language", "en")),
                Map.class);
        assertEquals(HttpStatus.OK, createResp.getStatusCode());
        Map<String, Object> prompt = (Map<String, Object>) createResp.getBody().get("prompt");
        assertNotNull(prompt);
        String promptId = (String) prompt.get("id");
        assertNotNull(promptId);
        assertEquals("greeting-prompt", prompt.get("name"));
        assertEquals("1", prompt.get("version"));
        assertEquals("Hello {{name}}, welcome to {{service}}!", prompt.get("template"));

        // Get
        ResponseEntity<Map> getResp = restTemplate.getForEntity(
                api("/prompts/get?id={id}"), Map.class, promptId);
        assertEquals(HttpStatus.OK, getResp.getStatusCode());
        Map<String, Object> retrieved = (Map<String, Object>) getResp.getBody().get("prompt");
        assertEquals("greeting-prompt", retrieved.get("name"));
        assertEquals("A greeting template", retrieved.get("description"));

        // List
        ResponseEntity<Map> listResp = restTemplate.getForEntity(api("/prompts/list"), Map.class);
        assertEquals(HttpStatus.OK, listResp.getStatusCode());
        List<Map<String, Object>> prompts = (List<Map<String, Object>>) listResp.getBody().get("prompts");
        assertTrue(prompts.stream().anyMatch(p -> promptId.equals(p.get("id"))));

        // Delete
        restTemplate.postForEntity(api("/prompts/delete"), Map.of("id", promptId), Map.class);

        // Verify deleted
        ResponseEntity<Map> afterDelete = restTemplate.getForEntity(
                api("/prompts/get?id={id}"), Map.class, promptId);
        assertNotEquals(HttpStatus.OK, afterDelete.getStatusCode());
    }

    // =========================================================================
    // 9. Scorer Workflow via REST
    // =========================================================================

    @Test
    @Order(9)
    @SuppressWarnings("unchecked")
    public void testScorerWorkflowViaREST() throws Exception {
        MlflowClient client = mlflow();
        String expId = client.createExperiment("scorer_workflow_" + System.currentTimeMillis());

        // Register scorer v1
        ResponseEntity<Map> reg1 = restTemplate.postForEntity(
                api("/scorers/register"),
                Map.of("experiment_id", expId,
                        "name", "accuracy_scorer",
                        "serialized_scorer", "{\"type\":\"accuracy\",\"threshold\":0.9}"),
                Map.class);
        assertEquals(HttpStatus.OK, reg1.getStatusCode());
        Map<String, Object> scorer1 = (Map<String, Object>) reg1.getBody().get("scorer");
        assertEquals("accuracy_scorer", scorer1.get("scorerName"));
        assertEquals(1, scorer1.get("scorerVersion"));

        // Register scorer v2 (same name → auto-increment version)
        ResponseEntity<Map> reg2 = restTemplate.postForEntity(
                api("/scorers/register"),
                Map.of("experiment_id", expId,
                        "name", "accuracy_scorer",
                        "serialized_scorer", "{\"type\":\"accuracy\",\"threshold\":0.95}"),
                Map.class);
        Map<String, Object> scorer2 = (Map<String, Object>) reg2.getBody().get("scorer");
        assertEquals(2, scorer2.get("scorerVersion"));

        // Register a different scorer
        restTemplate.postForEntity(
                api("/scorers/register"),
                Map.of("experiment_id", expId,
                        "name", "f1_scorer",
                        "serialized_scorer", "{\"type\":\"f1\"}"),
                Map.class);

        // List scorers (latest versions)
        ResponseEntity<Map> listResp = restTemplate.getForEntity(
                api("/scorers/list?experiment_id={expId}"), Map.class, expId);
        List<Map<String, Object>> scorers = (List<Map<String, Object>>) listResp.getBody().get("scorers");
        assertEquals(2, scorers.size());

        // List versions of a scorer
        ResponseEntity<Map> versionsResp = restTemplate.getForEntity(
                api("/scorers/versions?experiment_id={expId}&name={name}"),
                Map.class, expId, "accuracy_scorer");
        List<Map<String, Object>> versions = (List<Map<String, Object>>) versionsResp.getBody().get("scorers");
        assertEquals(2, versions.size());

        // Get specific version
        ResponseEntity<Map> getResp = restTemplate.getForEntity(
                api("/scorers/get?experiment_id={expId}&name={name}&version={v}"),
                Map.class, expId, "accuracy_scorer", 1);
        Map<String, Object> specific = (Map<String, Object>) getResp.getBody().get("scorer");
        assertEquals("{\"type\":\"accuracy\",\"threshold\":0.9}", specific.get("serializedScorer"));

        // Delete specific version
        restTemplate.delete(api("/scorers/delete?experiment_id={expId}&name={name}&version={v}"),
                expId, "accuracy_scorer", 1);

        // Verify v2 still exists
        ResponseEntity<Map> v2Resp = restTemplate.getForEntity(
                api("/scorers/get?experiment_id={expId}&name={name}&version={v}"),
                Map.class, expId, "accuracy_scorer", 2);
        assertEquals(HttpStatus.OK, v2Resp.getStatusCode());
    }

    // =========================================================================
    // 10. Gateway Route Workflow via REST
    // =========================================================================

    @Test
    @Order(10)
    @SuppressWarnings("unchecked")
    public void testGatewayRouteWorkflowViaREST() {
        // Create
        ResponseEntity<Map> createResp = restTemplate.postForEntity(
                api("/gateway/routes"),
                Map.of("name", "completions-route",
                        "routeType", "llm/v1/completions",
                        "modelName", "gpt-4",
                        "modelProvider", "openai",
                        "config", Map.of("max_tokens", 2048)),
                Map.class);
        assertEquals(HttpStatus.OK, createResp.getStatusCode());

        // Get
        ResponseEntity<Map> getResp = restTemplate.getForEntity(
                api("/gateway/routes/{name}"), Map.class, "completions-route");
        assertEquals(HttpStatus.OK, getResp.getStatusCode());
        Map<String, Object> route = (Map<String, Object>) getResp.getBody().get("route");
        assertEquals("completions-route", route.get("name"));
        assertEquals("gpt-4", route.get("modelName"));

        // Update (PATCH)
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, Object>> patchEntity = new HttpEntity<>(
                Map.of("model_name", "gpt-4-turbo"), headers);
        ResponseEntity<Map> patchResp = restTemplate.exchange(
                api("/gateway/routes/{name}"), HttpMethod.PATCH, patchEntity, Map.class, "completions-route");
        assertEquals(HttpStatus.OK, patchResp.getStatusCode());
        Map<String, Object> updatedRoute = (Map<String, Object>) patchResp.getBody().get("route");
        assertEquals("gpt-4-turbo", updatedRoute.get("modelName"));

        // List
        ResponseEntity<Map> listResp = restTemplate.getForEntity(api("/gateway/routes"), Map.class);
        List<Map<String, Object>> routes = (List<Map<String, Object>>) listResp.getBody().get("routes");
        assertTrue(routes.stream().anyMatch(r -> "completions-route".equals(r.get("name"))));
    }

    // =========================================================================
    // 11. Gateway Secret Workflow via REST
    // =========================================================================

    @Test
    @Order(11)
    @SuppressWarnings("unchecked")
    public void testGatewaySecretWorkflowViaREST() {
        // Create
        ResponseEntity<Map> createResp = restTemplate.postForEntity(
                api("/gateway/secrets/create"),
                Map.of("secret_name", "openai-api-key",
                        "secret_value", List.of(Map.of("key", "api_key", "value", "sk-test-123")),
                        "provider", "openai",
                        "auth_config", List.of(Map.of("key", "header", "value", "Authorization")),
                        "created_by", "admin"),
                Map.class);
        assertEquals(HttpStatus.OK, createResp.getStatusCode());
        Map<String, Object> secret = (Map<String, Object>) createResp.getBody().get("secret");
        String secretId = (String) secret.get("secretId");
        assertNotNull(secretId);
        assertEquals("openai-api-key", secret.get("secretName"));
        // Values should be masked
        List<Map<String, String>> masked = (List<Map<String, String>>) secret.get("maskedValues");
        assertEquals("********", masked.get(0).get("value"));

        // Get by ID
        ResponseEntity<Map> byIdResp = restTemplate.getForEntity(
                api("/gateway/secrets/get?secret_id={id}"), Map.class, secretId);
        assertEquals(HttpStatus.OK, byIdResp.getStatusCode());
        assertEquals(secretId, ((Map<String, Object>) byIdResp.getBody().get("secret")).get("secretId"));

        // Get by name
        ResponseEntity<Map> byNameResp = restTemplate.getForEntity(
                api("/gateway/secrets/get?secret_name={name}"), Map.class, "openai-api-key");
        assertEquals(HttpStatus.OK, byNameResp.getStatusCode());
        assertEquals(secretId, ((Map<String, Object>) byNameResp.getBody().get("secret")).get("secretId"));

        // Update
        ResponseEntity<Map> updateResp = restTemplate.postForEntity(
                api("/gateway/secrets/update"),
                Map.of("secret_id", secretId,
                        "secret_value", List.of(Map.of("key", "api_key", "value", "sk-new-456")),
                        "updated_by", "admin"),
                Map.class);
        assertEquals(HttpStatus.OK, updateResp.getStatusCode());

        // List all
        ResponseEntity<Map> listAllResp = restTemplate.getForEntity(
                api("/gateway/secrets/list"), Map.class);
        List<Map<String, Object>> allSecrets = (List<Map<String, Object>>) listAllResp.getBody().get("secrets");
        assertTrue(allSecrets.stream().anyMatch(s -> secretId.equals(s.get("secretId"))));

        // List by provider
        ResponseEntity<Map> listByProvider = restTemplate.getForEntity(
                api("/gateway/secrets/list?provider={p}"), Map.class, "openai");
        List<Map<String, Object>> openaiSecrets = (List<Map<String, Object>>) listByProvider.getBody().get("secrets");
        assertTrue(openaiSecrets.stream().anyMatch(s -> secretId.equals(s.get("secretId"))));

        // Delete
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, String>> deleteEntity = new HttpEntity<>(Map.of("secret_id", secretId), headers);
        restTemplate.exchange(api("/gateway/secrets/delete"), HttpMethod.DELETE, deleteEntity, Void.class);
    }

    // =========================================================================
    // 12. Gateway Endpoint Workflow via REST
    // =========================================================================

    @Test
    @Order(12)
    @SuppressWarnings("unchecked")
    public void testGatewayEndpointWorkflowViaREST() {
        // Attach primary model (auto-creates endpoint)
        ResponseEntity<Map> attachResp = restTemplate.postForEntity(
                api("/gateway/endpoints/models/attach"),
                Map.of("endpoint_id", "ep-workflow",
                        "model_definition_id", "model-def-1",
                        "linkage_type", "PRIMARY",
                        "weight", 1.0,
                        "fallback_order", 0,
                        "created_by", "admin"),
                Map.class);
        assertEquals(HttpStatus.OK, attachResp.getStatusCode());
        Map<String, Object> mapping = (Map<String, Object>) attachResp.getBody().get("mapping");
        String mappingId = (String) mapping.get("mappingId");
        assertNotNull(mappingId);

        // Attach fallback model
        ResponseEntity<Map> fallbackResp = restTemplate.postForEntity(
                api("/gateway/endpoints/models/attach"),
                Map.of("endpoint_id", "ep-workflow",
                        "model_definition_id", "model-def-2",
                        "linkage_type", "FALLBACK",
                        "weight", 0.0,
                        "fallback_order", 1,
                        "created_by", "admin"),
                Map.class);
        assertEquals(HttpStatus.OK, fallbackResp.getStatusCode());

        // Set tag
        restTemplate.postForEntity(
                api("/gateway/endpoints/set-tag"),
                Map.of("endpoint_id", "ep-workflow", "key", "env", "value", "production"),
                Void.class);

        // Overwrite tag
        restTemplate.postForEntity(
                api("/gateway/endpoints/set-tag"),
                Map.of("endpoint_id", "ep-workflow", "key", "env", "value", "staging"),
                Void.class);

        // Delete tag
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<Map<String, String>> deleteTagEntity = new HttpEntity<>(
                Map.of("endpoint_id", "ep-workflow", "key", "env"), headers);
        restTemplate.exchange(api("/gateway/endpoints/delete-tag"),
                HttpMethod.DELETE, deleteTagEntity, Void.class);

        // Detach primary model
        restTemplate.postForEntity(
                api("/gateway/endpoints/models/detach"),
                Map.of("endpoint_id", "ep-workflow", "mapping_id", mappingId),
                Void.class);
    }

    // =========================================================================
    // 13. HBase Direct Verification for Key Operations
    // =========================================================================

    @Test
    @Order(13)
    public void testHBaseDirectVerification() throws Exception {
        MlflowClient client = mlflow();

        String expName = "hbase_verify_" + System.currentTimeMillis();
        String expId = client.createExperiment(expName);

        // Verify experiment in HBase
        try (Table expTable = hbaseConnection.getTable(TableName.valueOf("mlflow_experiments"))) {
            Result result = expTable.get(new Get(Bytes.toBytes(expId)));
            assertFalse(result.isEmpty());
            assertEquals(expName,
                    Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
            assertEquals("active",
                    Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("lifecycle_stage"))));
        }

        // Verify name index in HBase
        try (Table indexTable = hbaseConnection.getTable(TableName.valueOf("mlflow_experiments_name_index"))) {
            Result result = indexTable.get(new Get(Bytes.toBytes(expName)));
            assertFalse(result.isEmpty());
            assertEquals(expId,
                    Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("experiment_id"))));
        }

        // Create run and verify in HBase
        Service.RunInfo runInfo = client.createRun(expId);
        String runId = runInfo.getRunId();

        try (Table runTable = hbaseConnection.getTable(TableName.valueOf("mlflow_runs"))) {
            Result result = runTable.get(new Get(Bytes.toBytes(runId)));
            assertFalse(result.isEmpty());
            assertEquals(expId,
                    Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("experiment_id"))));
        }
    }

    // =========================================================================
    // 14. Error Cases via REST
    // =========================================================================

    @Test
    @Order(14)
    public void testResourceNotFoundErrors() {
        ResponseEntity<Map> expResp = restTemplate.getForEntity(
                api("/experiments/get?experiment_id={id}"), Map.class, "nonexistent");
        assertNotEquals(HttpStatus.OK, expResp.getStatusCode());

        ResponseEntity<Map> runResp = restTemplate.getForEntity(
                api("/runs/get?run_id={id}"), Map.class, "nonexistent");
        assertNotEquals(HttpStatus.OK, runResp.getStatusCode());

        ResponseEntity<Map> modelResp = restTemplate.getForEntity(
                api("/registered-models/get?name={name}"), Map.class, "nonexistent");
        assertNotEquals(HttpStatus.OK, modelResp.getStatusCode());

        ResponseEntity<Map> promptResp = restTemplate.getForEntity(
                api("/prompts/get?id={id}"), Map.class, "nonexistent");
        assertNotEquals(HttpStatus.OK, promptResp.getStatusCode());
    }

    // =========================================================================
    // 15. Validation Errors via REST
    // =========================================================================

    @Test
    @Order(15)
    public void testValidationErrors() {
        ResponseEntity<Map> emptyExpResp = restTemplate.postForEntity(
                api("/experiments/create"), Map.of("name", ""), Map.class);
        assertNotEquals(HttpStatus.OK, emptyExpResp.getStatusCode());

        ResponseEntity<Map> emptyModelResp = restTemplate.postForEntity(
                api("/registered-models/create"), Map.of("name", ""), Map.class);
        assertNotEquals(HttpStatus.OK, emptyModelResp.getStatusCode());

        ResponseEntity<Map> emptyPromptResp = restTemplate.postForEntity(
                api("/prompts/create"), Map.of("name", "", "template", "test"), Map.class);
        assertNotEquals(HttpStatus.OK, emptyPromptResp.getStatusCode());

        ResponseEntity<Map> emptySecretResp = restTemplate.postForEntity(
                api("/gateway/secrets/create"), Map.of("secret_name", ""), Map.class);
        assertNotEquals(HttpStatus.OK, emptySecretResp.getStatusCode());
    }

    // =========================================================================
    // 16. Serving Endpoints (ping, version, metadata)
    // =========================================================================

    @Test
    @Order(16)
    @SuppressWarnings("unchecked")
    public void testServingEndpoints() {
        ResponseEntity<Map> pingResp = restTemplate.getForEntity("/api/ping", Map.class);
        assertEquals(HttpStatus.OK, pingResp.getStatusCode());
        assertEquals("healthy", pingResp.getBody().get("status"));

        ResponseEntity<Map> versionResp = restTemplate.getForEntity("/api/version", Map.class);
        assertEquals(HttpStatus.OK, versionResp.getStatusCode());
        assertNotNull(versionResp.getBody().get("version"));

        ResponseEntity<Map> metadataResp = restTemplate.getForEntity("/api/metadata", Map.class);
        assertEquals(HttpStatus.OK, metadataResp.getStatusCode());
        assertNotNull(metadataResp.getBody().get("model_name"));
    }

    // =========================================================================
    // Helper methods
    // =========================================================================

    private void uploadArtifact(String runId, String path, String fileName, String content) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);

        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();
        body.add("file", new ByteArrayResource(content.getBytes()) {
            @Override
            public String getFilename() {
                return fileName;
            }
        });

        String url = api("/artifacts/upload?run_id={runId}");
        if (path != null) {
            url += "&path={path}";
        }

        HttpEntity<MultiValueMap<String, Object>> requestEntity = new HttpEntity<>(body, headers);

        ResponseEntity<Map> resp;
        if (path != null) {
            resp = restTemplate.exchange(url, HttpMethod.POST, requestEntity, Map.class, runId, path);
        } else {
            resp = restTemplate.exchange(url, HttpMethod.POST, requestEntity, Map.class, runId);
        }
        assertEquals(HttpStatus.OK, resp.getStatusCode());
    }

    private byte[] downloadArtifact(String runId, String path) {
        ResponseEntity<byte[]> resp = restTemplate.getForEntity(
                api("/artifacts/download?run_id={runId}&path={path}"),
                byte[].class, runId, path);
        assertEquals(HttpStatus.OK, resp.getStatusCode());
        return resp.getBody();
    }
}
