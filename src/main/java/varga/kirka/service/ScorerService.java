package varga.kirka.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import varga.kirka.model.Scorer;
import varga.kirka.repo.ScorerRepository;
import varga.kirka.security.SecurityContextHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ScorerService {

    private static final String RESOURCE_TYPE = "scorer";

    private final ScorerRepository scorerRepository;

    private final SecurityContextHelper securityContextHelper;

    public Scorer registerScorer(String experimentId, String name, String serializedScorer) throws IOException {
        if (experimentId == null || experimentId.isBlank()) {
            throw new IllegalArgumentException("experiment_id must not be empty");
        }
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Scorer name must not be empty");
        }
        return scorerRepository.registerScorer(experimentId, name, serializedScorer);
    }

    public List<Scorer> listScorers(String experimentId) throws IOException {
        List<Scorer> scorers = scorerRepository.listScorers(experimentId);
        return scorers.stream()
                .filter(scorer -> securityContextHelper.canRead(RESOURCE_TYPE, scorer.getScorerId(),
                        scorer.getOwner(), Map.of()))
                .collect(Collectors.toList());
    }

    public List<Scorer> listScorerVersions(String experimentId, String name) throws IOException {
        List<Scorer> scorers = scorerRepository.listScorerVersions(experimentId, name);
        return scorers.stream()
                .filter(scorer -> securityContextHelper.canRead(RESOURCE_TYPE, scorer.getScorerId(),
                        scorer.getOwner(), Map.of()))
                .collect(Collectors.toList());
    }

    public Scorer getScorer(String experimentId, String name, Integer version) throws IOException {
        Scorer scorer = scorerRepository.getScorer(experimentId, name, version);
        if (scorer == null) {
            throw new ResourceNotFoundException("Scorer", experimentId + "/" + name);
        }
        securityContextHelper.checkReadAccess(RESOURCE_TYPE, scorer.getScorerId(), scorer.getOwner(), Map.of());
        return scorer;
    }

    public void deleteScorer(String experimentId, String name, Integer version) throws IOException {
        Scorer scorer = scorerRepository.getScorer(experimentId, name, version);
        if (scorer == null) {
            throw new ResourceNotFoundException("Scorer", experimentId + "/" + name);
        }
        securityContextHelper.checkDeleteAccess(RESOURCE_TYPE, scorer.getScorerId(), scorer.getOwner(), Map.of());
        scorerRepository.deleteScorer(experimentId, name, version);
    }
}
