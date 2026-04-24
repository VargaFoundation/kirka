package varga.kirka.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import varga.kirka.model.Run;
import varga.kirka.model.RunInfo;
import varga.kirka.repo.RunRepository;
import varga.kirka.security.SecurityContextHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class RunServiceTest {

    @Mock
    private RunRepository runRepository;

    @Mock
    private SecurityContextHelper securityContextHelper;

    @InjectMocks
    private RunService runService;

    @BeforeEach
    void setUpAuthz() {
        when(securityContextHelper.getCurrentUser()).thenReturn("alice");
        when(securityContextHelper.tagsToMap(any(), any(), any())).thenReturn(Map.of());
        when(securityContextHelper.canRead(any(), any(), any(), any())).thenReturn(true);
    }

    /** A run that the service can load so that permission checks have something to evaluate. */
    private Run existingRun(String runId) {
        return Run.builder().info(RunInfo.builder().runId(runId).experimentId("exp1").userId("alice").build()).build();
    }

    @Test
    public void testCreateRun() throws IOException {
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "val1");
        long startTime = System.currentTimeMillis();

        Run run = runService.createRun("exp1", "user1", startTime, tags);

        assertNotNull(run);
        assertNotNull(run.getInfo().getRunId());
        assertEquals("exp1", run.getInfo().getExperimentId());
        assertEquals(varga.kirka.model.RunStatus.RUNNING, run.getInfo().getStatus());
        assertEquals(startTime, run.getInfo().getStartTime());
        assertEquals(1, run.getData().getTags().size());
        assertEquals("tag1", run.getData().getTags().get(0).getKey());
        assertEquals("val1", run.getData().getTags().get(0).getValue());
        verify(runRepository, times(1)).createRun(any(Run.class));
    }

    @Test
    public void testGetRun() throws IOException {
        when(runRepository.getRun("run123")).thenReturn(existingRun("run123"));
        Run result = runService.getRun("run123");
        assertNotNull(result);
        assertEquals("run123", result.getInfo().getRunId());
    }

    @Test
    public void testUpdateRun() throws IOException {
        when(runRepository.getRun("run1")).thenReturn(existingRun("run1"));
        runService.updateRun("run1", "FINISHED", 1000L);
        verify(runRepository, times(1)).updateRun("run1", "FINISHED", 1000L);
    }

    @Test
    public void testLogMetric() throws IOException {
        when(runRepository.getRun("run1")).thenReturn(existingRun("run1"));
        runService.logMetric("run1", "accuracy", 0.95, 1000L, 1L);
        verify(runRepository, times(1)).logBatch(eq("run1"), anyList(), isNull(), isNull());
    }

    @Test
    public void testLogParameter() throws IOException {
        when(runRepository.getRun("run1")).thenReturn(existingRun("run1"));
        runService.logParameter("run1", "lr", "0.01");
        verify(runRepository, times(1)).logBatch(eq("run1"), isNull(), anyList(), isNull());
    }

    @Test
    public void testSetTag() throws IOException {
        when(runRepository.getRun("run1")).thenReturn(existingRun("run1"));
        runService.setTag("run1", "t1", "v1");
        verify(runRepository).setTag("run1", "t1", "v1");
    }

    @Test
    public void testDeleteTag() throws IOException {
        when(runRepository.getRun("run1")).thenReturn(existingRun("run1"));
        runService.deleteTag("run1", "t1");
        verify(runRepository).deleteTag("run1", "t1");
    }

    @Test
    public void testDeleteRestoreRun() throws IOException {
        when(runRepository.getRun("run1")).thenReturn(existingRun("run1"));
        runService.deleteRun("run1");
        verify(runRepository).deleteRun("run1");

        runService.restoreRun("run1");
        verify(runRepository).restoreRun("run1");
    }

    @Test
    public void testSearchRuns() throws IOException {
        when(runRepository.searchRuns(anyList(), anyString(), anyString())).thenReturn(Collections.emptyList());
        // A well-formed filter is parsed through FilterParser; pass a valid MLFlow expression
        // rather than the bare word "filter" the legacy test used.
        List<Run> results = runService.searchRuns(List.of("exp1"),
                "tags.env = 'prod'", "ACTIVE_ONLY");
        assertTrue(results.isEmpty());
    }

    @Test
    public void testGetMetricHistory() throws IOException {
        when(runRepository.getRun("run1")).thenReturn(existingRun("run1"));
        runService.getMetricHistory("run1", "accuracy");
        verify(runRepository).getMetricHistory("run1", "accuracy");
    }
}
