package varga.kirka.repo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import varga.kirka.model.Scorer;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = {
    "spring.main.allow-bean-definition-overriding=true",
    "security.kerberos.enabled=false"
})
@Import(AbstractHBaseIntegrationTest.HBaseTestConfig.class)
public class ScorerRepositoryIntegrationTest extends AbstractHBaseIntegrationTest {

    @Autowired
    private ScorerRepository scorerRepository;

    @Test
    public void testRegisterAndGetScorer() throws IOException {
        String experimentId = "1";
        String name = "my_scorer";
        String serialized = "{\"type\": \"custom\"}";

        Scorer registered = scorerRepository.registerScorer(experimentId, name, serialized);
        assertNotNull(registered);
        assertEquals(name, registered.getScorerName());
        assertEquals(1, registered.getScorerVersion());

        Scorer retrieved = scorerRepository.getScorer(experimentId, name, 1);
        assertNotNull(retrieved);
        assertEquals(name, retrieved.getScorerName());
        assertEquals(serialized, retrieved.getSerializedScorer());
    }

    @Test
    public void testListScorers() throws IOException {
        String experimentId = "2";
        scorerRepository.registerScorer(experimentId, "s1", "{}");
        scorerRepository.registerScorer(experimentId, "s1", "{}"); // Version 2
        scorerRepository.registerScorer(experimentId, "s2", "{}");

        List<Scorer> scorers = scorerRepository.listScorers(experimentId);
        assertNotNull(scorers);
        assertEquals(2, scorers.size());
        assertTrue(scorers.stream().anyMatch(s -> s.getScorerName().equals("s1") && s.getScorerVersion() == 2));
        assertTrue(scorers.stream().anyMatch(s -> s.getScorerName().equals("s2") && s.getScorerVersion() == 1));
    }

    @Test
    public void testDeleteScorer() throws IOException {
        String experimentId = "3";
        scorerRepository.registerScorer(experimentId, "to_delete", "{}");
        
        scorerRepository.deleteScorer(experimentId, "to_delete", 1);
        Scorer retrieved = scorerRepository.getScorer(experimentId, "to_delete", 1);
        assertNull(retrieved);
    }
}
