package varga.kirka.repo;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import varga.kirka.model.Run;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = {
    "spring.main.allow-bean-definition-overriding=true",
    "security.kerberos.enabled=false"
})
@Import(AbstractHBaseIntegrationTest.HBaseTestConfig.class)
public class RunRepositoryIntegrationTest extends AbstractHBaseIntegrationTest {

    @Autowired
    private RunRepository runRepository;

    @Test
    public void testCreateAndGetRun() throws IOException {
        String runId = "run-123";
        Run run = Run.builder()
                .info(varga.kirka.model.RunInfo.builder()
                        .runId(runId)
                        .experimentId("exp-1")
                        .status(varga.kirka.model.RunStatus.RUNNING)
                        .startTime(System.currentTimeMillis())
                        .artifactUri("hdfs:///tmp/run-123")
                        .build())
                .data(varga.kirka.model.RunData.builder()
                        .tags(List.of(new varga.kirka.model.RunTag("t1", "v1")))
                        .build())
                .build();

        runRepository.createRun(run);

        Run retrieved = runRepository.getRun(runId);
        assertNotNull(retrieved);
        assertEquals(runId, retrieved.getInfo().getRunId());
        assertEquals(varga.kirka.model.RunStatus.RUNNING, retrieved.getInfo().getStatus());
        assertTrue(retrieved.getData().getTags().stream().anyMatch(t -> t.getKey().equals("t1") && t.getValue().equals("v1")));
    }

    @Test
    public void testUpdateRun() throws IOException {
        String runId = "run-update";
        Run run = Run.builder().info(varga.kirka.model.RunInfo.builder().runId(runId).experimentId("exp-1").status(varga.kirka.model.RunStatus.RUNNING).build()).build();
        runRepository.createRun(run);

        long endTime = System.currentTimeMillis();
        runRepository.updateRun(runId, "FINISHED", endTime);

        Run retrieved = runRepository.getRun(runId);
        assertEquals(varga.kirka.model.RunStatus.FINISHED, retrieved.getInfo().getStatus());
        assertEquals(endTime, retrieved.getInfo().getEndTime());
    }

    @Test
    public void testLogBatch() throws IOException {
        String runId = "run-batch";
        Run run = Run.builder().info(varga.kirka.model.RunInfo.builder().runId(runId).experimentId("exp-1").build()).build();
        runRepository.createRun(run);

        List<varga.kirka.model.Metric> metrics = List.of(new varga.kirka.model.Metric("m1", 0.9, System.currentTimeMillis(), 1));
        List<varga.kirka.model.Param> params = List.of(new varga.kirka.model.Param("p1", "v1"));
        runRepository.logBatch(runId, metrics, params, null);

        Run retrieved = runRepository.getRun(runId);
        assertTrue(retrieved.getData().getMetrics().stream().anyMatch(m -> m.getKey().equals("m1")));
        assertTrue(retrieved.getData().getParams().stream().anyMatch(p -> p.getKey().equals("p1")));
    }
}
