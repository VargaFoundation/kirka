package varga.kirka.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import varga.kirka.model.Experiment;
import varga.kirka.repo.ExperimentRepository;
import varga.kirka.security.SecurityContextHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class ExperimentServiceTest {

    @Mock
    private ExperimentRepository experimentRepository;

    @Mock
    private SecurityContextHelper securityContextHelper;

    @InjectMocks
    private ExperimentService experimentService;

    @BeforeEach
    void setUpAuthz() {
        // Permissive stubs for the authz helper: tests focus on business logic, not on
        // Ranger/owner policies which have dedicated coverage.
        when(securityContextHelper.getCurrentUser()).thenReturn("alice");
        when(securityContextHelper.tagsToMap(any(), any(), any())).thenReturn(Map.of());
        when(securityContextHelper.canRead(any(), any(), any(), any())).thenReturn(true);
    }

    @Test
    public void testCreateExperiment() throws IOException {
        String id = experimentService.createExperiment("test-experiment", "hdfs:///tmp", null);
        assertNotNull(id);
        verify(experimentRepository, times(1)).createExperiment(any(Experiment.class));
    }

    @Test
    public void testGetExperiment() throws IOException {
        Experiment experiment = Experiment.builder().experimentId("123").name("test").build();
        when(experimentRepository.getExperiment("123")).thenReturn(experiment);

        Experiment result = experimentService.getExperiment("123");

        assertEquals("test", result.getName());
        assertEquals("123", result.getExperimentId());
    }

    @Test
    public void testListExperiments() throws IOException {
        when(experimentRepository.listExperiments()).thenReturn(Collections.emptyList());
        assertTrue(experimentService.listExperiments().isEmpty());
    }

    @Test
    public void testSearchExperimentsFilter() throws IOException {
        Experiment exp1 = Experiment.builder().experimentId("1").name("exp1").lifecycleStage("active").build();
        Experiment exp2 = Experiment.builder().experimentId("2").name("exp2").lifecycleStage("deleted").build();
        Experiment exp3 = Experiment.builder().experimentId("3").name("exp3").lifecycleStage("active").build();
        when(experimentRepository.listExperiments()).thenReturn(List.of(exp1, exp2, exp3));

        List<Experiment> active = experimentService.searchExperiments("active_only", null, null);
        assertEquals(2, active.size());
        assertTrue(active.contains(exp1));
        assertTrue(active.contains(exp3));

        List<Experiment> deleted = experimentService.searchExperiments("deleted_only", null, null);
        assertEquals(1, deleted.size());
        assertEquals("exp2", deleted.get(0).getName());

        List<Experiment> filtered = experimentService.searchExperiments("ALL", null, "name = 'exp2'");
        assertEquals(1, filtered.size());
        assertEquals("exp2", filtered.get(0).getName());

        List<Experiment> limited = experimentService.searchExperiments("ALL", 2, null);
        assertEquals(2, limited.size());
    }

    @Test
    public void testUpdateExperiment() throws IOException {
        when(experimentRepository.getExperiment("123"))
                .thenReturn(Experiment.builder().experimentId("123").name("Old Name").build());
        experimentService.updateExperiment("123", "New Name");
        verify(experimentRepository).updateExperiment("123", "New Name");
    }

    @Test
    public void testDeleteRestoreExperiment() throws IOException {
        when(experimentRepository.getExperiment("123"))
                .thenReturn(Experiment.builder().experimentId("123").name("exp").build());
        experimentService.deleteExperiment("123");
        verify(experimentRepository).deleteExperiment("123");

        experimentService.restoreExperiment("123");
        verify(experimentRepository).restoreExperiment("123");
    }

    @Test
    public void testSetExperimentTag() throws IOException {
        when(experimentRepository.getExperiment("123"))
                .thenReturn(Experiment.builder().experimentId("123").name("exp").build());
        experimentService.setExperimentTag("123", "key", "value");
        verify(experimentRepository).setExperimentTag("123", "key", "value");
    }
}
