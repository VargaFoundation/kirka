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
import varga.kirka.model.RegisteredModel;

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
public class ModelRegistryRepositoryIntegrationTest extends AbstractHBaseIntegrationTest {

    @Autowired
    private ModelRegistryRepository modelRegistryRepository;

    @Test
    public void testCreateAndGetRegisteredModel() throws IOException {
        String name = "test-model";
        modelRegistryRepository.createRegisteredModel(name);

        // Verification of data in mock to debug
        // System.out.println("[DEBUG_LOG] Data: " + data);
        
        RegisteredModel retrieved = modelRegistryRepository.getRegisteredModel(name);
        assertNotNull(retrieved);
        assertEquals(name, retrieved.getName());
    }

    @Test
    public void testCreateModelVersion() throws IOException {
        String modelName = "versioned-model";
        modelRegistryRepository.createRegisteredModel(modelName);
        
        varga.kirka.model.ModelVersion version = varga.kirka.model.ModelVersion.builder()
                .name(modelName)
                .version("1")
                .creationTimestamp(System.currentTimeMillis())
                .currentStage("None")
                .source("hdfs:///tmp")
                .runId("run-1")
                .status(varga.kirka.model.ModelVersionStatus.READY)
                .build();
        
        modelRegistryRepository.createModelVersion(version);
        
        varga.kirka.model.ModelVersion retrieved = modelRegistryRepository.getModelVersion(modelName, "1");
        assertNotNull(retrieved);
        assertEquals("1", retrieved.getVersion());
        assertEquals(varga.kirka.model.ModelVersionStatus.READY, retrieved.getStatus());
    }

    @Test
    public void testUpdateAndDelete() throws IOException {
        String name = "upd-del-model";
        modelRegistryRepository.createRegisteredModel(name);
        modelRegistryRepository.updateRegisteredModel(name, "new desc");
        
        RegisteredModel retrieved = modelRegistryRepository.getRegisteredModel(name);
        assertEquals("new desc", retrieved.getDescription());

        modelRegistryRepository.deleteRegisteredModel(name);
        assertNull(modelRegistryRepository.getRegisteredModel(name));
    }
}
