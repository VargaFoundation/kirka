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
    "hadoop.hdfs.uri=file:///",
    "hbase.zookeeper.quorum=localhost",
    "hbase.zookeeper.property.clientPort=2181",
    "security.kerberos.enabled=false"
})
@Import({ModelRegistryRepositoryIntegrationTest.MockHBaseConfig.class, ModelRegistryRepositoryIntegrationTest.MockHdfsConfig.class})
public class ModelRegistryRepositoryIntegrationTest {

    @TestConfiguration
    static class MockHdfsConfig {
        @Bean("fileSystem")
        @Primary
        public org.apache.hadoop.fs.FileSystem testFileSystem() throws IOException {
            return mock(org.apache.hadoop.fs.FileSystem.class);
        }
    }

    @TestConfiguration
    static class MockHBaseConfig {
        @Bean("hbaseConnection")
        @Primary
        public Connection testHbaseConnection() throws IOException {
            Connection mockConn = mock(Connection.class);
            Table mockTable = mock(Table.class);
            when(mockConn.getTable(any(TableName.class))).thenReturn(mockTable);
            
            final Map<String, Map<String, Map<String, byte[]>>> data = new HashMap<>();
            
            // Mock Put
            doAnswer(invocation -> {
                Put put = invocation.getArgument(0);
                String rowKey = Bytes.toString(put.getRow());
                data.putIfAbsent(rowKey, new HashMap<>());
                for (java.util.List<org.apache.hadoop.hbase.Cell> cells : put.getFamilyCellMap().values()) {
                    for (org.apache.hadoop.hbase.Cell cell : cells) {
                        String family = Bytes.toString(org.apache.hadoop.hbase.CellUtil.cloneFamily(cell));
                        String qualifier = Bytes.toString(org.apache.hadoop.hbase.CellUtil.cloneQualifier(cell));
                        byte[] value = org.apache.hadoop.hbase.CellUtil.cloneValue(cell);
                        data.get(rowKey).putIfAbsent(family, new HashMap<>());
                        data.get(rowKey).get(family).put(qualifier, value);
                    }
                }
                return null;
            }).when(mockTable).put(any(Put.class));

            // Mock Get
            when(mockTable.get(any(Get.class))).thenAnswer(invocation -> {
                Get get = invocation.getArgument(0);
                String rowKey = Bytes.toString(get.getRow());
                Map<String, Map<String, byte[]>> rowData = data.get(rowKey);
                if (rowData == null) return Result.EMPTY_RESULT;
                List<org.apache.hadoop.hbase.Cell> cells = new ArrayList<>();
                for (Map.Entry<String, Map<String, byte[]>> familyEntry : rowData.entrySet()) {
                    byte[] family = Bytes.toBytes(familyEntry.getKey());
                    for (Map.Entry<String, byte[]> qualEntry : familyEntry.getValue().entrySet()) {
                        byte[] qualifier = Bytes.toBytes(qualEntry.getKey());
                        cells.add(org.apache.hadoop.hbase.CellBuilderFactory.create(org.apache.hadoop.hbase.CellBuilderType.DEEP_COPY)
                                .setRow(get.getRow())
                                .setFamily(family)
                                .setQualifier(qualifier)
                                .setTimestamp(System.currentTimeMillis())
                                .setType(org.apache.hadoop.hbase.Cell.Type.Put)
                                .setValue(qualEntry.getValue())
                                .build());
                    }
                }
                Result res = Result.create(cells);
                // System.out.println("[DEBUG_LOG] Get Result for " + rowKey + ": " + res);
                return res;
            });

            // Mock Scanner
            when(mockTable.getScanner(any(Scan.class))).thenAnswer(invocation -> {
                Scan scan = invocation.getArgument(0);
                byte[] prefixBytes = scan.getStartRow(); 
                String prefix = prefixBytes != null ? Bytes.toString(prefixBytes) : "";
                
                ResultScanner scanner = mock(ResultScanner.class);
                List<Result> allResults = new ArrayList<>();
                for(Map.Entry<String, Map<String, Map<String, byte[]>>> entry : data.entrySet()) {
                    if (!prefix.isEmpty() && !entry.getKey().startsWith(prefix)) {
                        continue;
                    }
                    
                    byte[] row = Bytes.toBytes(entry.getKey());
                    List<org.apache.hadoop.hbase.Cell> cells = new ArrayList<>();
                    for (Map.Entry<String, Map<String, byte[]>> familyEntry : entry.getValue().entrySet()) {
                        byte[] family = Bytes.toBytes(familyEntry.getKey());
                        for (Map.Entry<String, byte[]> qualEntry : familyEntry.getValue().entrySet()) {
                             cells.add(org.apache.hadoop.hbase.CellBuilderFactory.create(org.apache.hadoop.hbase.CellBuilderType.DEEP_COPY)
                                .setRow(row)
                                .setFamily(family)
                                .setQualifier(Bytes.toBytes(qualEntry.getKey()))
                                .setTimestamp(System.currentTimeMillis())
                                .setType(org.apache.hadoop.hbase.Cell.Type.Put)
                                .setValue(qualEntry.getValue())
                                .build());
                        }
                    }
                    if (!cells.isEmpty()) {
                        allResults.add(Result.create(cells));
                    }
                }
                
                final java.util.Iterator<Result> it = allResults.iterator();
                when(scanner.next()).thenAnswer(inv -> it.hasNext() ? it.next() : null);
                when(scanner.iterator()).thenReturn(it);
                return scanner;
            });

            return mockConn;
        }
    }

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
                .build();
        
        modelRegistryRepository.createModelVersion(version);
        
        varga.kirka.model.ModelVersion retrieved = modelRegistryRepository.getModelVersion(modelName, "1");
        assertNotNull(retrieved);
        assertEquals("1", retrieved.getVersion());
    }
}
