package varga.kirka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GatewayEndpointModelMapping {
    private String mappingId;
    private String endpointId;
    private String modelDefinitionId;
    private GatewayModelDefinition modelDefinition;
    private float weight;
    private long createdAt;
    private String createdBy;
    private GatewayModelLinkageType linkageType;
    private Integer fallbackOrder;
}
