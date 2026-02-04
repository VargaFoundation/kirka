package varga.kirka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Experiment {
    private String experimentId;
    private String name;
    private String artifactLocation;
    private String lifecycleStage;
    private long lastUpdateTime;
    private long creationTime;
    private List<ExperimentTag> tags;
    /** Owner of the experiment (user who created it) */
    private String owner;
}
