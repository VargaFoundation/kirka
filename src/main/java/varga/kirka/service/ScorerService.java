package varga.kirka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import varga.kirka.model.Scorer;
import varga.kirka.repo.ScorerRepository;
import varga.kirka.security.SecurityContextHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class ScorerService {

    private static final String RESOURCE_TYPE = "scorer";

    @Autowired
    private ScorerRepository scorerRepository;

    @Autowired
    private SecurityContextHelper securityContextHelper;

    public Scorer registerScorer(String experimentId, String name, String serializedScorer) throws IOException {
        // Owner will be set by the repository based on current user
        return scorerRepository.registerScorer(experimentId, name, serializedScorer);
    }

    public List<Scorer> listScorers(String experimentId) throws IOException {
        List<Scorer> scorers = scorerRepository.listScorers(experimentId);
        // Filter scorers based on read access
        return scorers.stream()
                .filter(scorer -> securityContextHelper.canRead(RESOURCE_TYPE, scorer.getScorerId(), 
                        scorer.getOwner(), Map.of()))
                .collect(Collectors.toList());
    }

    public List<Scorer> listScorerVersions(String experimentId, String name) throws IOException {
        List<Scorer> scorers = scorerRepository.listScorerVersions(experimentId, name);
        // Filter scorers based on read access
        return scorers.stream()
                .filter(scorer -> securityContextHelper.canRead(RESOURCE_TYPE, scorer.getScorerId(), 
                        scorer.getOwner(), Map.of()))
                .collect(Collectors.toList());
    }

    public Scorer getScorer(String experimentId, String name, Integer version) throws IOException {
        Scorer scorer = scorerRepository.getScorer(experimentId, name, version);
        if (scorer != null) {
            securityContextHelper.checkReadAccess(RESOURCE_TYPE, scorer.getScorerId(), scorer.getOwner(), Map.of());
        }
        return scorer;
    }

    public void deleteScorer(String experimentId, String name, Integer version) throws IOException {
        Scorer scorer = scorerRepository.getScorer(experimentId, name, version);
        if (scorer != null) {
            securityContextHelper.checkDeleteAccess(RESOURCE_TYPE, scorer.getScorerId(), scorer.getOwner(), Map.of());
        }
        scorerRepository.deleteScorer(experimentId, name, version);
    }
}
