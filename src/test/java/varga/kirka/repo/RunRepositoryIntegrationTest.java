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
    "hadoop.hdfs.uri=file:///",
    "hbase.zookeeper.quorum=localhost",
    "hbase.zookeeper.property.clientPort=2181",
    "security.kerberos.enabled=false"
})
@Import({RunRepositoryIntegrationTest.MockHBaseConfig.class, RunRepositoryIntegrationTest.MockHdfsConfig.class})
public class RunRepositoryIntegrationTest {

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
                        cells.add(org.apache.hadoop.hbase.CellBuilderFactory.create(org.apache.hadoop.hbase.CellBuilderType.DEEP_COPY)
                                .setRow(get.getRow())
                                .setFamily(family)
                                .setQualifier(Bytes.toBytes(qualEntry.getKey()))
                                .setTimestamp(System.currentTimeMillis())
                                .setType(org.apache.hadoop.hbase.Cell.Type.Put)
                                .setValue(qualEntry.getValue())
                                .build());
                    }
                }
                return Result.create(cells);
            });

            return mockConn;
        }
    }

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
