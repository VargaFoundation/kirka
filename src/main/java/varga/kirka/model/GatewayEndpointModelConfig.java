package varga.kirka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GatewayEndpointModelConfig {
    private String modelDefinitionId;
    private GatewayModelLinkageType linkageType;
    private float weight;
    private Integer fallbackOrder;
}
