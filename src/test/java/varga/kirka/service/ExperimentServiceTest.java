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

    @Test
    public void testSearchExperimentsFilter() throws IOException {
        Experiment exp1 = Experiment.builder().experimentId("1").name("exp1").lifecycleStage("active").build();
        Experiment exp2 = Experiment.builder().experimentId("2").name("exp2").lifecycleStage("deleted").build();
        Experiment exp3 = Experiment.builder().experimentId("3").name("exp3").lifecycleStage("active").build();
        when(experimentRepository.listExperiments()).thenReturn(List.of(exp1, exp2, exp3));

        // Test view_type active_only
        List<Experiment> active = experimentService.searchExperiments("active_only", null, null);
        assertEquals(2, active.size());
        assertTrue(active.contains(exp1));
        assertTrue(active.contains(exp3));

        // Test view_type deleted_only
        List<Experiment> deleted = experimentService.searchExperiments("deleted_only", null, null);
        assertEquals(1, deleted.size());
        assertEquals("exp2", deleted.get(0).getName());

        // Test filter by name
        List<Experiment> filtered = experimentService.searchExperiments("ALL", null, "name = 'exp2'");
        assertEquals(1, filtered.size());
        assertEquals("exp2", filtered.get(0).getName());

        // Test max_results
        List<Experiment> limited = experimentService.searchExperiments("ALL", 2, null);
        assertEquals(2, limited.size());
    }

    @Test
    public void testUpdateExperiment() throws IOException {
        experimentService.updateExperiment("123", "New Name");
        verify(experimentRepository).updateExperiment("123", "New Name");
    }

    @Test
    public void testDeleteRestoreExperiment() throws IOException {
        experimentService.deleteExperiment("123");
        verify(experimentRepository).deleteExperiment("123");

        experimentService.restoreExperiment("123");
        verify(experimentRepository).restoreExperiment("123");
    }

    @Test
    public void testSetExperimentTag() throws IOException {
        experimentService.setExperimentTag("123", "key", "value");
        verify(experimentRepository).setExperimentTag("123", "key", "value");
    }
}