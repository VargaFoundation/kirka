package varga.kirka.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import varga.kirka.model.Run;
import varga.kirka.repo.RunRepository;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class RunServiceTest {

    @Mock
    private RunRepository runRepository;

    @InjectMocks
    private RunService runService;

    @Test
    public void testCreateRun() throws IOException {
        String experimentId = "exp1";
        String userId = "user1";
        long startTime = System.currentTimeMillis();
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "val1");

        Run run = runService.createRun(experimentId, userId, startTime, tags);

        assertNotNull(run);
        assertNotNull(run.getInfo().getRunId());
        assertEquals(experimentId, run.getInfo().getExperimentId());
        assertEquals(varga.kirka.model.RunStatus.RUNNING, run.getInfo().getStatus());
        assertEquals(startTime, run.getInfo().getStartTime());
        assertEquals(1, run.getData().getTags().size());
        assertEquals("tag1", run.getData().getTags().get(0).getKey());
        assertEquals("val1", run.getData().getTags().get(0).getValue());
        verify(runRepository, times(1)).createRun(any(Run.class));
    }

    @Test
    public void testGetRun() throws IOException {
        String runId = "run123";
        Run run = Run.builder().info(varga.kirka.model.RunInfo.builder().runId(runId).experimentId("exp1").build()).build();
        when(runRepository.getRun(runId)).thenReturn(run);

        Run result = runService.getRun(runId);

        assertNotNull(result);
        assertEquals(runId, result.getInfo().getRunId());
    }

    @Test
    public void testUpdateRun() throws IOException {
        runService.updateRun("run1", "FINISHED", 1000L);
        verify(runRepository, times(1)).updateRun("run1", "FINISHED", 1000L);
    }

    @Test
    public void testLogMetric() throws IOException {
        runService.logMetric("run1", "accuracy", 0.95, 1000L, 1L);
        verify(runRepository, times(1)).logBatch(eq("run1"), anyList(), isNull(), isNull());
    }

    @Test
    public void testLogParameter() throws IOException {
        runService.logParameter("run1", "lr", "0.01");
        verify(runRepository, times(1)).logBatch(eq("run1"), isNull(), anyList(), isNull());
    }

    @Test
    public void testSetTag() throws IOException {
        runService.setTag("run1", "t1", "v1");
        verify(runRepository).setTag("run1", "t1", "v1");
    }

    @Test
    public void testDeleteTag() throws IOException {
        runService.deleteTag("run1", "t1");
        verify(runRepository).deleteTag("run1", "t1");
    }

    @Test
    public void testDeleteRestoreRun() throws IOException {
        runService.deleteRun("run1");
        verify(runRepository).deleteRun("run1");

        runService.restoreRun("run1");
        verify(runRepository).restoreRun("run1");
    }

    @Test
    public void testSearchRuns() throws IOException {
        when(runRepository.searchRuns(anyList(), anyString(), anyString())).thenReturn(Collections.emptyList());
        List<Run> results = runService.searchRuns(List.of("exp1"), "filter", "active");
        assertTrue(results.isEmpty());
    }

    @Test
    public void testGetMetricHistory() throws IOException {
        runService.getMetricHistory("run1", "accuracy");
        verify(runRepository).getMetricHistory("run1", "accuracy");
    }
}
