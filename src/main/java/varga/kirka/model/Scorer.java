package varga.kirka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Scorer {
    private int experimentId;
    private String scorerName;
    private int scorerVersion;
    private String serializedScorer;
    private long creationTime;
    private String scorerId;
}
