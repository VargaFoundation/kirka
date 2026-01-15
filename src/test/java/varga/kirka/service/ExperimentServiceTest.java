package varga.kirka.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import varga.kirka.model.Experiment;
import varga.kirka.repo.ExperimentRepository;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ExperimentServiceTest {

    @Mock
    private ExperimentRepository experimentRepository;

    @InjectMocks
    private ExperimentService experimentService;

    @Test
    public void testCreateExperiment() throws IOException {
        String name = "test-experiment";
        String artifactLocation = "hdfs:///tmp";
        
        String id = experimentService.createExperiment(name, artifactLocation, null);
        
        assertNotNull(id);
        verify(experimentRepository, times(1)).createExperiment(any(Experiment.class));
    }

    @Test
    public void testGetExperiment() throws IOException {
        String id = "123";
        Experiment experiment = Experiment.builder().experimentId(id).name("test").build();
        when(experimentRepository.getExperiment(id)).thenReturn(experiment);
        
        Experiment result = experimentService.getExperiment(id);
        
        assertEquals("test", result.getName());
        assertEquals(id, result.getExperimentId());
    }

    @Test
    public void testListExperiments() throws IOException {
        when(experimentRepository.listExperiments()).thenReturn(Collections.emptyList());
        
        List<Experiment> results = experimentService.listExperiments();
        
        assertTrue(results.isEmpty());
    }
}