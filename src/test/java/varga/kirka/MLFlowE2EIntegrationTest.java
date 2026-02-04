package varga.kirka;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.mlflow.tracking.MlflowClient;
import org.mlflow.tracking.RunsPage;
import org.mlflow.api.proto.Service;
import org.mlflow.api.proto.Service.ViewType;
import varga.kirka.repo.AbstractHBaseIntegrationTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.main.allow-bean-definition-overriding=true",
                "security.kerberos.enabled=false"
        }
)
public class MLFlowE2EIntegrationTest extends AbstractHBaseIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private Connection hbaseConnection;

    @Test
    public void testFullMLFlowLifecycle() throws Exception {
        MlflowClient client = new MlflowClient("http://localhost:" + port);

        // 1) Création d'une expérience via le client Java MLflow
        String experimentName = "E2E_Experiment_" + System.currentTimeMillis();
        String experimentId = client.createExperiment(experimentName);
        assertNotNull(experimentId);

        // Vérifier dans HBase que l'expérience est bien créée
        try (Table table = hbaseConnection.getTable(TableName.valueOf("mlflow_experiments"))) {
            Get get = new Get(Bytes.toBytes(experimentId));
            Result result = table.get(get);
            org.junit.jupiter.api.Assertions.assertFalse(result.isEmpty());
            assertEquals(experimentName, Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"))));
        }

        // 2) Création d'un run et log de paramètres/métriques
        Service.RunInfo runInfo = client.createRun(experimentId);
        String runId = runInfo.getRunId();
        assertTrue(runId != null && !runId.isBlank());

        client.logParam(runId, "param1", "5");
        client.logMetric(runId, "accuracy", 0.95);
        client.setTag(runId, "project", "kirka");

        Service.Run run = client.getRun(runId);
        assertEquals(runId, run.getInfo().getRunId());

        // 3) Recherche de runs (validation end-to-end : API de recherche)
        RunsPage runsPage = client.searchRuns(List.of(experimentId), "", ViewType.ACTIVE_ONLY, 100);
        assertTrue(runsPage.getItems().size() >= 1);
    }
}
