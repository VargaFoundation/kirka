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
public class RegisteredModel {
    private String name;
    private long creationTimestamp;
    private long lastUpdatedTimestamp;
    private String userId;
    private String description;
    private List<ModelVersion> latestVersions;
    private List<RegisteredModelTag> tags;
    private List<RegisteredModelAlias> aliases;
    private String deploymentJobId;
    private State deploymentJobState;
}
