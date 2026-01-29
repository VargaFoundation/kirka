package varga.kirka.service;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import varga.kirka.repo.ArtifactRepository;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ArtifactServiceTest {

    @Mock
    private ArtifactRepository artifactRepository;

    @InjectMocks
    private ArtifactService artifactService;

    @Test
    public void testListArtifacts() throws IOException {
        String path = "hdfs:///test";
        varga.kirka.model.FileInfo info = varga.kirka.model.FileInfo.builder()
                .path("hdfs:///test/file.txt")
                .isDir(false)
                .build();
        when(artifactRepository.listArtifacts(path)).thenReturn(List.of(info));

        List<varga.kirka.model.FileInfo> results = artifactService.listArtifacts(path);

        assertEquals(1, results.size());
        assertEquals("hdfs:///test/file.txt", results.get(0).getPath());
    }

    @Test
    public void testDeleteArtifact() throws IOException {
        String path = "hdfs:///test/file.txt";
        artifactService.deleteArtifact(path);
        verify(artifactRepository).deleteArtifact(path);
    }

    @Test
    public void testUploadArtifact() throws IOException {
        String path = "hdfs:///test/file.txt";
        ByteArrayInputStream is = new ByteArrayInputStream("content".getBytes());
        
        artifactService.uploadArtifact(path, is);
        
        verify(artifactRepository, times(1)).uploadArtifact(path, is);
    }

    @Test
    public void testDownloadArtifact() throws IOException {
        String path = "hdfs:///test/file.txt";
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        
        artifactService.downloadArtifact(path, os);
        
        verify(artifactRepository, times(1)).downloadArtifact(path, os);
    }
}
