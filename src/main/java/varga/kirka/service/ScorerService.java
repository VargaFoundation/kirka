package varga.kirka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import varga.kirka.model.Scorer;
import varga.kirka.repo.ScorerRepository;

import java.io.IOException;
import java.util.List;

@Service
public class ScorerService {

    @Autowired
    private ScorerRepository scorerRepository;

    public Scorer registerScorer(String experimentId, String name, String serializedScorer) throws IOException {
        return scorerRepository.registerScorer(experimentId, name, serializedScorer);
    }

    public List<Scorer> listScorers(String experimentId) throws IOException {
        return scorerRepository.listScorers(experimentId);
    }

    public List<Scorer> listScorerVersions(String experimentId, String name) throws IOException {
        return scorerRepository.listScorerVersions(experimentId, name);
    }

    public Scorer getScorer(String experimentId, String name, Integer version) throws IOException {
        return scorerRepository.getScorer(experimentId, name, version);
    }

    public void deleteScorer(String experimentId, String name, Integer version) throws IOException {
        scorerRepository.deleteScorer(experimentId, name, version);
    }
}
