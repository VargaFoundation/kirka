package varga.kirka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GatewayModelDefinition {
    private String modelDefinitionId;
    private String name;
    private String secretId;
    private String secretName;
    private String provider;
    private String modelName;
    private long createdAt;
    private long lastUpdatedAt;
    private String createdBy;
    private String lastUpdatedBy;
}
