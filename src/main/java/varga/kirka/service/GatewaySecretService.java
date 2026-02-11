package varga.kirka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import varga.kirka.model.AuthConfigEntry;
import varga.kirka.model.GatewaySecretInfo;
import varga.kirka.model.MaskedValuesEntry;
import varga.kirka.model.SecretValueEntry;
import varga.kirka.repo.GatewaySecretRepository;
import varga.kirka.security.SecurityContextHelper;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class GatewaySecretService {

    private static final String RESOURCE_TYPE = "secret";

    private final GatewaySecretRepository gatewaySecretRepository;

    private final SecurityContextHelper securityContextHelper;

    public GatewaySecretInfo createSecret(String name, List<SecretValueEntry> value, String provider, List<AuthConfigEntry> authConfig, String createdBy) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Secret name must not be empty");
        }
        String id = UUID.randomUUID().toString();
        String currentUser = securityContextHelper.getCurrentUser();
        String owner = currentUser != null ? currentUser : createdBy;

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
                .createdBy(owner)
                .lastUpdatedBy(owner)
                .authConfig(authConfig)
                .build();

        try {
            gatewaySecretRepository.saveSecret(secretInfo, value);
        } catch (IOException e) {
            log.error("Failed to save secret to HBase", e);
            throw new RuntimeException("Failed to create secret", e);
        }
        return secretInfo;
    }

    public GatewaySecretInfo updateSecret(String id, List<SecretValueEntry> value, List<AuthConfigEntry> authConfig, String updatedBy) {
        GatewaySecretInfo existing;
        try {
            existing = gatewaySecretRepository.getSecretById(id);
        } catch (IOException e) {
            log.error("Failed to get secret from HBase", e);
            throw new RuntimeException("Failed to get secret", e);
        }

        if (existing == null) {
            throw new ResourceNotFoundException("GatewaySecret", id);
        }

        securityContextHelper.checkWriteAccess(RESOURCE_TYPE, id, existing.getCreatedBy(), Map.of());

        existing.setLastUpdatedAt(System.currentTimeMillis());
        existing.setLastUpdatedBy(updatedBy);

        List<SecretValueEntry> secretValues = value;
        if (value != null && !value.isEmpty()) {
            List<MaskedValuesEntry> maskedValues = new ArrayList<>();
            for (SecretValueEntry entry : value) {
                MaskedValuesEntry masked = new MaskedValuesEntry();
                masked.setKey(entry.getKey());
                masked.setValue("********");
                maskedValues.add(masked);
            }
            existing.setMaskedValues(maskedValues);
        } else {
            try {
                secretValues = gatewaySecretRepository.getSecretValues(id);
            } catch (IOException e) {
                log.error("Failed to get secret values from HBase", e);
                throw new RuntimeException("Failed to get secret values", e);
            }
        }

        if (authConfig != null) {
            existing.setAuthConfig(authConfig);
        }

        try {
            gatewaySecretRepository.saveSecret(existing, secretValues);
        } catch (IOException e) {
            log.error("Failed to update secret in HBase", e);
            throw new RuntimeException("Failed to update secret", e);
        }

        return existing;
    }

    public void deleteSecret(String id) {
        try {
            GatewaySecretInfo secret = gatewaySecretRepository.getSecretById(id);
            if (secret == null) {
                throw new ResourceNotFoundException("GatewaySecret", id);
            }
            securityContextHelper.checkDeleteAccess(RESOURCE_TYPE, id, secret.getCreatedBy(), Map.of());
            gatewaySecretRepository.deleteSecret(id);
        } catch (IOException e) {
            log.error("Failed to delete secret from HBase", e);
            throw new RuntimeException("Failed to delete secret", e);
        }
    }

    public GatewaySecretInfo getSecret(String id, String name) {
        try {
            GatewaySecretInfo secret = null;
            if (id != null) {
                secret = gatewaySecretRepository.getSecretById(id);
            } else if (name != null) {
                secret = gatewaySecretRepository.getSecretByName(name);
            }
            if (secret == null) {
                throw new ResourceNotFoundException("GatewaySecret", id != null ? id : name);
            }
            securityContextHelper.checkReadAccess(RESOURCE_TYPE, secret.getSecretId(), secret.getCreatedBy(), Map.of());
            return secret;
        } catch (IOException e) {
            log.error("Failed to get secret from HBase", e);
            throw new RuntimeException("Failed to get secret", e);
        }
    }

    public List<GatewaySecretInfo> listSecrets(String provider) {
        try {
            List<GatewaySecretInfo> secrets = gatewaySecretRepository.listSecrets(provider);
            return secrets.stream()
                    .filter(secret -> securityContextHelper.canRead(RESOURCE_TYPE, secret.getSecretId(),
                            secret.getCreatedBy(), Map.of()))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            log.error("Failed to list secrets from HBase", e);
            throw new RuntimeException("Failed to list secrets", e);
        }
    }
}
