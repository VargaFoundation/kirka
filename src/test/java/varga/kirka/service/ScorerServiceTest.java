package varga.kirka.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import varga.kirka.model.Scorer;
import varga.kirka.repo.ScorerRepository;
import varga.kirka.security.SecurityContextHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ScorerServiceTest {

    @Mock
    private ScorerRepository scorerRepository;

    @Mock
    private SecurityContextHelper securityContextHelper;

    @InjectMocks
    private ScorerService scorerService;

    @Test
    public void testRegisterScorer() throws IOException {
        String experimentId = "123";
        String name = "test-scorer";
        String serializedScorer = "serialized_data";

        Scorer scorer = Scorer.builder()
                .scorerId("scorer-id")
                .experimentId("123")
                .scorerName(name)
                .scorerVersion(1)
                .serializedScorer(serializedScorer)
                .creationTime(System.currentTimeMillis())
                .build();

        when(scorerRepository.registerScorer(experimentId, name, serializedScorer)).thenReturn(scorer);

        Scorer result = scorerService.registerScorer(experimentId, name, serializedScorer);

        assertNotNull(result);
        assertEquals(name, result.getScorerName());
        assertEquals(1, result.getScorerVersion());
        verify(scorerRepository).registerScorer(experimentId, name, serializedScorer);
    }

    @Test
    public void testListScorers() throws IOException {
        String experimentId = "123";
        Scorer scorer1 = Scorer.builder().scorerId("1").experimentId("123").scorerName("scorer1").scorerVersion(1).build();
        Scorer scorer2 = Scorer.builder().scorerId("2").experimentId("123").scorerName("scorer2").scorerVersion(1).build();

        when(scorerRepository.listScorers(experimentId)).thenReturn(List.of(scorer1, scorer2));
        when(securityContextHelper.canRead(eq("scorer"), anyString(), any(), any())).thenReturn(true);

        List<Scorer> results = scorerService.listScorers(experimentId);

        assertEquals(2, results.size());
        verify(scorerRepository).listScorers(experimentId);
    }

    @Test
    public void testListScorersEmpty() throws IOException {
        String experimentId = "123";
        when(scorerRepository.listScorers(experimentId)).thenReturn(Collections.emptyList());

        List<Scorer> results = scorerService.listScorers(experimentId);

        assertTrue(results.isEmpty());
        verify(scorerRepository).listScorers(experimentId);
    }

    @Test
    public void testListScorerVersions() throws IOException {
        String experimentId = "123";
        String name = "test-scorer";
        Scorer v1 = Scorer.builder().scorerId("1").experimentId("123").scorerName(name).scorerVersion(1).build();
        Scorer v2 = Scorer.builder().scorerId("2").experimentId("123").scorerName(name).scorerVersion(2).build();

        when(scorerRepository.listScorerVersions(experimentId, name)).thenReturn(List.of(v1, v2));
        when(securityContextHelper.canRead(eq("scorer"), anyString(), any(), any())).thenReturn(true);

        List<Scorer> results = scorerService.listScorerVersions(experimentId, name);

        assertEquals(2, results.size());
        verify(scorerRepository).listScorerVersions(experimentId, name);
    }

    @Test
    public void testGetScorer() throws IOException {
        String experimentId = "123";
        String name = "test-scorer";
        Integer version = 1;

        Scorer scorer = Scorer.builder()
                .scorerId("scorer-id")
                .experimentId("123")
                .scorerName(name)
                .scorerVersion(version)
                .build();

        when(scorerRepository.getScorer(experimentId, name, version)).thenReturn(scorer);

        Scorer result = scorerService.getScorer(experimentId, name, version);

        assertNotNull(result);
        assertEquals(name, result.getScorerName());
        assertEquals(version, result.getScorerVersion());
        verify(scorerRepository).getScorer(experimentId, name, version);
    }

    @Test
    public void testGetScorerLatestVersion() throws IOException {
        String experimentId = "123";
        String name = "test-scorer";

        Scorer scorer = Scorer.builder()
                .scorerId("scorer-id")
                .experimentId("123")
                .scorerName(name)
                .scorerVersion(3)
                .build();

        when(scorerRepository.getScorer(experimentId, name, null)).thenReturn(scorer);

        Scorer result = scorerService.getScorer(experimentId, name, null);

        assertNotNull(result);
        assertEquals(3, result.getScorerVersion());
        verify(scorerRepository).getScorer(experimentId, name, null);
    }

    @Test
    public void testGetScorerNotFound() throws IOException {
        String experimentId = "123";
        String name = "nonexistent";

        when(scorerRepository.getScorer(experimentId, name, null)).thenReturn(null);

        assertThrows(ResourceNotFoundException.class, () -> {
            scorerService.getScorer(experimentId, name, null);
        });
    }

    @Test
    public void testDeleteScorer() throws IOException {
        String experimentId = "123";
        String name = "test-scorer";
        Integer version = 1;

        Scorer scorer = Scorer.builder()
                .scorerId("scorer-id")
                .experimentId("123")
                .scorerName(name)
                .scorerVersion(version)
                .build();

        when(scorerRepository.getScorer(experimentId, name, version)).thenReturn(scorer);

        scorerService.deleteScorer(experimentId, name, version);

        verify(scorerRepository).deleteScorer(experimentId, name, version);
    }

    @Test
    public void testDeleteScorerNotFound() throws IOException {
        String experimentId = "123";
        String name = "nonexistent";

        when(scorerRepository.getScorer(experimentId, name, null)).thenReturn(null);

        assertThrows(ResourceNotFoundException.class, () -> {
            scorerService.deleteScorer(experimentId, name, null);
        });
    }

    @Test
    public void testRegisterScorerWithEmptyName() {
        assertThrows(IllegalArgumentException.class, () -> {
            scorerService.registerScorer("123", "", "data");
        });
    }

    @Test
    public void testRegisterScorerWithNullExperimentId() {
        assertThrows(IllegalArgumentException.class, () -> {
            scorerService.registerScorer(null, "name", "data");
        });
    }
}
