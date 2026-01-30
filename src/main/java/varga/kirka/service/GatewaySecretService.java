package varga.kirka.service;

import org.springframework.stereotype.Service;
import varga.kirka.model.AuthConfigEntry;
import varga.kirka.model.GatewaySecretInfo;
import varga.kirka.model.MaskedValuesEntry;
import varga.kirka.model.SecretValueEntry;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class GatewaySecretService {
    private final Map<String, GatewaySecretInfo> secretsById = new HashMap<>();
    private final Map<String, List<SecretValueEntry>> secretValues = new HashMap<>();

    public GatewaySecretInfo createSecret(String name, List<SecretValueEntry> value, String provider, List<AuthConfigEntry> authConfig, String createdBy) {
        String id = UUID.randomUUID().toString();
        
        List<MaskedValuesEntry> maskedValues = new ArrayList<>();
        if (value != null) {
            for (SecretValueEntry entry : value) {
                MaskedValuesEntry masked = new MaskedValuesEntry();
                masked.setKey(entry.getKey());
                masked.setValue("********");
                maskedValues.add(masked);
            }
        }

        GatewaySecretInfo secretInfo = GatewaySecretInfo.builder()
                .secretId(id)
                .secretName(name)
                .maskedValues(maskedValues)
                .createdAt(System.currentTimeMillis())
                .lastUpdatedAt(System.currentTimeMillis())
                .provider(provider)
                .createdBy(createdBy)
                .lastUpdatedBy(createdBy)
                .authConfig(authConfig)
                .build();

        secretsById.put(id, secretInfo);
        secretValues.put(id, value);
        return secretInfo;
    }

    public GatewaySecretInfo updateSecret(String id, List<SecretValueEntry> value, List<AuthConfigEntry> authConfig, String updatedBy) {
        GatewaySecretInfo existing = secretsById.get(id);
        if (existing == null) {
            throw new RuntimeException("Secret not found: " + id);
        }

        existing.setLastUpdatedAt(System.currentTimeMillis());
        existing.setLastUpdatedBy(updatedBy);

        if (value != null && !value.isEmpty()) {
            secretValues.put(id, value);
            List<MaskedValuesEntry> maskedValues = new ArrayList<>();
            for (SecretValueEntry entry : value) {
                MaskedValuesEntry masked = new MaskedValuesEntry();
                masked.setKey(entry.getKey());
                masked.setValue("********");
                maskedValues.add(masked);
            }
            existing.setMaskedValues(maskedValues);
        }

        if (authConfig != null) {
            existing.setAuthConfig(authConfig);
        }

        return existing;
    }

    public void deleteSecret(String id) {
        secretsById.remove(id);
        secretValues.remove(id);
    }

    public GatewaySecretInfo getSecret(String id, String name) {
        if (id != null) {
            return secretsById.get(id);
        }
        if (name != null) {
            for (GatewaySecretInfo s : secretsById.values()) {
                if (name.equals(s.getSecretName())) {
                    return s;
                }
            }
        }
        return null;
    }

    public List<GatewaySecretInfo> listSecrets(String provider) {
        if (provider == null) {
            return new ArrayList<>(secretsById.values());
        }
        List<GatewaySecretInfo> filtered = new ArrayList<>();
        for (GatewaySecretInfo s : secretsById.values()) {
            if (provider.equals(s.getProvider())) {
                filtered.add(s);
            }
        }
        return filtered;
    }
}
