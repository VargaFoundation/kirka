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
public class ModelVersion {
    private String name;
    private String version;
    private long creationTimestamp;
    private long lastUpdatedTimestamp;
    private String userId;
    private String currentStage;
    private String description;
    private String source;
    private String runId;
    private ModelVersionStatus status;
    private String statusMessage;
    private List<ModelVersionTag> tags;
    private String runLink;
    private List<String> aliases;
    private String modelId;
    private List<ModelParam> modelParams;
    private List<ModelMetric> modelMetrics;
    private ModelVersionDeploymentJobState deploymentJobState;
}
