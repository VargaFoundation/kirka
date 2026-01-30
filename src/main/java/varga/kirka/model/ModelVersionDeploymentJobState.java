package varga.kirka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ModelVersionDeploymentJobState {
    private String jobId;
    private String runId;
    private State jobState;
    private DeploymentJobRunState runState;
    private String currentTaskName;
}
