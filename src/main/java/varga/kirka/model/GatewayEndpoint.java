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
public class GatewayEndpoint {
    private String endpointId;
    private String name;
    private long createdAt;
    private long lastUpdatedAt;
    private List<GatewayEndpointModelMapping> modelMappings;
    private String createdBy;
    private String lastUpdatedBy;
    private List<GatewayEndpointTag> tags;
    private RoutingStrategy routingStrategy;
    private FallbackConfig fallbackConfig;
}
