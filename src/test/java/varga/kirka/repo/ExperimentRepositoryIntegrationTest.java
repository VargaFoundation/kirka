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
import varga.kirka.model.Experiment;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = {
    "spring.main.allow-bean-definition-overriding=true",
    "security.kerberos.enabled=false"
})
@Import(AbstractHBaseIntegrationTest.HBaseTestConfig.class)
public class ExperimentRepositoryIntegrationTest extends AbstractHBaseIntegrationTest {

    @Autowired
    private ExperimentRepository experimentRepository;

    @Test
    public void testCreateAndGetExperiment() throws IOException {
        String experimentId = "exp-123";
        Experiment experiment = Experiment.builder()
                .experimentId(experimentId)
                .name("Test Experiment")
                .artifactLocation("hdfs:///tmp")
                .lifecycleStage("active")
                .creationTime(123456789L)
                .lastUpdateTime(123456789L)
                .tags(List.of(new varga.kirka.model.ExperimentTag("key1", "value1")))
                .build();

        experimentRepository.createExperiment(experiment);

        Experiment retrieved = experimentRepository.getExperiment(experimentId);
        assertNotNull(retrieved);
        assertEquals("Test Experiment", retrieved.getName());
        assertNotNull(retrieved.getTags());
        assertEquals(1, retrieved.getTags().size());
        assertEquals("key1", retrieved.getTags().get(0).getKey());
        assertEquals("value1", retrieved.getTags().get(0).getValue());
    }

    @Test
    public void testGetExperimentByName() throws IOException {
        String name = "Unique Name";
        Experiment experiment = Experiment.builder()
                .experimentId("exp-unique")
                .name(name)
                .artifactLocation("hdfs:///tmp")
                .lifecycleStage("active")
                .build();
        experimentRepository.createExperiment(experiment);

        Experiment retrieved = experimentRepository.getExperimentByName(name);
        assertNotNull(retrieved);
        assertEquals(name, retrieved.getName());
    }

    @Test
    public void testUpdateExperiment() throws IOException {
        String experimentId = "exp-update";
        Experiment experiment = Experiment.builder()
                .experimentId(experimentId)
                .name("Old Name")
                .artifactLocation("hdfs:///tmp")
                .lifecycleStage("active")
                .build();
        experimentRepository.createExperiment(experiment);

        experimentRepository.updateExperiment(experimentId, "New Name");

        Experiment retrieved = experimentRepository.getExperiment(experimentId);
        assertEquals("New Name", retrieved.getName());
    }

    @Test
    public void testDeleteAndRestoreExperiment() throws IOException {
        String experimentId = "exp-del-rest";
        Experiment experiment = Experiment.builder()
                .experimentId(experimentId)
                .name("DelRest Experiment")
                .artifactLocation("hdfs:///tmp")
                .lifecycleStage("active")
                .build();
        experimentRepository.createExperiment(experiment);

        experimentRepository.deleteExperiment(experimentId);
        assertEquals("deleted", experimentRepository.getExperiment(experimentId).getLifecycleStage());

        experimentRepository.restoreExperiment(experimentId);
        assertEquals("active", experimentRepository.getExperiment(experimentId).getLifecycleStage());
    }

    @Test
    public void testSetExperimentTag() throws IOException {
        String experimentId = "exp-tag";
        Experiment experiment = Experiment.builder()
                .experimentId(experimentId)
                .name("Tag Experiment")
                .artifactLocation("hdfs:///tmp")
                .lifecycleStage("active")
                .build();
        experimentRepository.createExperiment(experiment);

        experimentRepository.setExperimentTag(experimentId, "new_key", "new_val");

        Experiment retrieved = experimentRepository.getExperiment(experimentId);
        boolean found = retrieved.getTags().stream().anyMatch(t -> t.getKey().equals("new_key") && t.getValue().equals("new_val"));
        assertTrue(found);
    }

    @Test
    public void testListExperiments() throws IOException {
        String experimentId = "exp-list-1";
        Experiment experiment = Experiment.builder()
                .experimentId(experimentId)
                .name("List Experiment")
                .artifactLocation("hdfs:///tmp/list")
                .lifecycleStage("active")
                .creationTime(System.currentTimeMillis())
                .lastUpdateTime(System.currentTimeMillis())
                .build();
        experimentRepository.createExperiment(experiment);

        List<Experiment> list = experimentRepository.listExperiments();
        assertNotNull(list);
        assertTrue(list.stream().anyMatch(e -> e.getExperimentId().equals(experimentId)));
    }
}
