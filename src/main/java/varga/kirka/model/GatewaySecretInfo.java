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
public class GatewaySecretInfo {
    private String secretId;
    private String secretName;
    private List<MaskedValuesEntry> maskedValues;
    private long createdAt;
    private long lastUpdatedAt;
    private String provider;
    private String createdBy;
    private String lastUpdatedBy;
    private List<AuthConfigEntry> authConfig;
}
