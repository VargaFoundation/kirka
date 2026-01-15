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
                .runId(runId)
                .experimentId("exp-1")
                .status("RUNNING")
                .startTime(System.currentTimeMillis())
                .artifactUri("hdfs:///tmp/run-123")
                .tags(Map.of("t1", "v1"))
                .build();

        runRepository.createRun(run);

        Run retrieved = runRepository.getRun(runId);
        assertNotNull(retrieved);
        assertEquals(runId, retrieved.getRunId());
        assertEquals("RUNNING", retrieved.getStatus());
        assertEquals("v1", retrieved.getTags().get("t1"));
    }
}
