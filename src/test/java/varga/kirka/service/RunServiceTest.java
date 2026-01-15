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
        assertNotNull(run.getRunId());
        assertEquals(experimentId, run.getExperimentId());
        assertEquals("RUNNING", run.getStatus());
        assertEquals(startTime, run.getStartTime());
        assertEquals(tags, run.getTags());
        verify(runRepository, times(1)).createRun(any(Run.class));
    }

    @Test
    public void testGetRun() throws IOException {
        String runId = "run123";
        Run run = Run.builder().runId(runId).experimentId("exp1").build();
        when(runRepository.getRun(runId)).thenReturn(run);

        Run result = runService.getRun(runId);

        assertNotNull(result);
        assertEquals(runId, result.getRunId());
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
    public void testSearchRuns() throws IOException {
        when(runRepository.searchRuns(anyList(), anyString(), anyString())).thenReturn(Collections.emptyList());
        List<Run> results = runService.searchRuns(List.of("exp1"), "filter", "active");
        assertTrue(results.isEmpty());
    }
}
