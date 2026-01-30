package varga.kirka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class GatewayEndpointBinding {
    private String endpointId;
    private String resourceType;
    private String resourceId;
    private long createdAt;
    private long lastUpdatedAt;
    private String createdBy;
    private String lastUpdatedBy;
    private String displayName;
}
