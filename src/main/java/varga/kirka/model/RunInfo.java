package varga.kirka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RunInfo {
    private String runId;
    private String runUuid;
    private String runName;
    private String experimentId;
    private String userId;
    private RunStatus status;
    private long startTime;
    private long endTime;
    private String artifactUri;
    private String lifecycleStage;
}
