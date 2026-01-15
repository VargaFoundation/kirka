package varga.kirka.repo;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = {
    "spring.main.allow-bean-definition-overriding=true",
    "hadoop.hdfs.uri=file:///",
    "hbase.zookeeper.quorum=localhost",
    "hbase.zookeeper.property.clientPort=2181",
    "security.kerberos.enabled=false"
})
@Import({ArtifactRepositoryIntegrationTest.MockHBaseConfig.class, ArtifactRepositoryIntegrationTest.MockHdfsConfig.class})
public class ArtifactRepositoryIntegrationTest {

    @TestConfiguration
    static class MockHdfsConfig {
        @Bean("fileSystem")
        @Primary
        public FileSystem testFileSystem() throws IOException {
            FileSystem fs = mock(FileSystem.class);
            when(fs.exists(any(Path.class))).thenReturn(true);
            return fs;
        }
    }

    @TestConfiguration
    static class MockHBaseConfig {
        @Bean("hbaseConnection")
        @Primary
        public Connection testHbaseConnection() throws IOException {
            return mock(Connection.class);
        }
    }

    @Autowired
    private ArtifactRepository artifactRepository;

    @Autowired
    private FileSystem fileSystem;

    @Test
    public void testExists() throws IOException {
        String path = "/tmp/test.txt";
        assertTrue(artifactRepository.exists(path));
        verify(fileSystem).exists(new Path(path));
    }

    @Test
    public void testDelete() throws IOException {
        String path = "/tmp/delete.txt";
        artifactRepository.deleteArtifact(path);
        verify(fileSystem).delete(new Path(path), true);
    }
}
