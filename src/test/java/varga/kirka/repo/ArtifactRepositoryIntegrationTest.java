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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(properties = {
    "spring.main.allow-bean-definition-overriding=true",
    "security.kerberos.enabled=false"
})
@Import(AbstractHBaseIntegrationTest.HBaseTestConfig.class)
public class ArtifactRepositoryIntegrationTest extends AbstractHBaseIntegrationTest {

    @Autowired
    private ArtifactRepository artifactRepository;

    @Autowired
    private FileSystem fileSystem;

    @Test
    public void testUploadAndExists() throws IOException {
        String path = "/tmp/test.txt";
        byte[] content = "Hello HBase".getBytes();
        artifactRepository.uploadArtifact(path, new ByteArrayInputStream(content));
        
        assertTrue(artifactRepository.exists(path));
    }

    @Test
    public void testDelete() throws IOException {
        String path = "/tmp/delete.txt";
        artifactRepository.uploadArtifact(path, new ByteArrayInputStream("to delete".getBytes()));
        assertTrue(artifactRepository.exists(path));
        
        artifactRepository.deleteArtifact(path);
        assertFalse(artifactRepository.exists(path));
    }
}
