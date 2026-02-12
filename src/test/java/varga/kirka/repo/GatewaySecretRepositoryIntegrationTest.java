package varga.kirka.repo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import varga.kirka.model.*;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = {
    "spring.main.allow-bean-definition-overriding=true",
    "security.kerberos.enabled=false"
})
@Import(AbstractHBaseIntegrationTest.HBaseTestConfig.class)
public class GatewaySecretRepositoryIntegrationTest extends AbstractHBaseIntegrationTest {

    @Autowired
    private GatewaySecretRepository gatewaySecretRepository;

    @Test
    public void testSaveAndGetSecretById() throws IOException {
        GatewaySecretInfo secret = GatewaySecretInfo.builder()
                .secretId("secret-1")
                .secretName("my-openai-key")
                .provider("openai")
                .createdAt(System.currentTimeMillis())
                .lastUpdatedAt(System.currentTimeMillis())
                .createdBy("admin")
                .lastUpdatedBy("admin")
                .maskedValues(List.of(
                        buildMaskedValue("api_key", "********")
                ))
                .authConfig(List.of(
                        buildAuthConfig("header", "Authorization")
                ))
                .build();

        List<SecretValueEntry> values = List.of(buildSecretValue("api_key", "sk-real-key-123"));

        gatewaySecretRepository.saveSecret(secret, values);

        GatewaySecretInfo retrieved = gatewaySecretRepository.getSecretById("secret-1");
        assertNotNull(retrieved);
        assertEquals("secret-1", retrieved.getSecretId());
        assertEquals("my-openai-key", retrieved.getSecretName());
        assertEquals("openai", retrieved.getProvider());
        assertEquals("admin", retrieved.getCreatedBy());
        assertNotNull(retrieved.getMaskedValues());
        assertEquals(1, retrieved.getMaskedValues().size());
        assertEquals("api_key", retrieved.getMaskedValues().get(0).getKey());
        assertEquals("********", retrieved.getMaskedValues().get(0).getValue());
        assertNotNull(retrieved.getAuthConfig());
        assertEquals(1, retrieved.getAuthConfig().size());
    }

    @Test
    public void testGetSecretByName() throws IOException {
        GatewaySecretInfo secret = GatewaySecretInfo.builder()
                .secretId("secret-by-name")
                .secretName("unique-secret-name")
                .provider("anthropic")
                .createdAt(System.currentTimeMillis())
                .lastUpdatedAt(System.currentTimeMillis())
                .createdBy("user1")
                .build();

        gatewaySecretRepository.saveSecret(secret, null);

        GatewaySecretInfo retrieved = gatewaySecretRepository.getSecretByName("unique-secret-name");
        assertNotNull(retrieved);
        assertEquals("secret-by-name", retrieved.getSecretId());
        assertEquals("unique-secret-name", retrieved.getSecretName());
    }

    @Test
    public void testGetSecretValues() throws IOException {
        List<SecretValueEntry> values = List.of(
                buildSecretValue("api_key", "sk-secret-abc"),
                buildSecretValue("org_id", "org-xyz")
        );

        GatewaySecretInfo secret = GatewaySecretInfo.builder()
                .secretId("secret-values-test")
                .secretName("values-test")
                .createdAt(System.currentTimeMillis())
                .lastUpdatedAt(System.currentTimeMillis())
                .build();

        gatewaySecretRepository.saveSecret(secret, values);

        List<SecretValueEntry> retrieved = gatewaySecretRepository.getSecretValues("secret-values-test");
        assertNotNull(retrieved);
        assertEquals(2, retrieved.size());
        assertTrue(retrieved.stream().anyMatch(v -> "api_key".equals(v.getKey()) && "sk-secret-abc".equals(v.getValue())));
        assertTrue(retrieved.stream().anyMatch(v -> "org_id".equals(v.getKey()) && "org-xyz".equals(v.getValue())));
    }

    @Test
    public void testListSecrets() throws IOException {
        gatewaySecretRepository.saveSecret(GatewaySecretInfo.builder()
                .secretId("list-secret-1")
                .secretName("list-s1")
                .provider("openai")
                .createdAt(System.currentTimeMillis())
                .lastUpdatedAt(System.currentTimeMillis())
                .build(), null);

        gatewaySecretRepository.saveSecret(GatewaySecretInfo.builder()
                .secretId("list-secret-2")
                .secretName("list-s2")
                .provider("anthropic")
                .createdAt(System.currentTimeMillis())
                .lastUpdatedAt(System.currentTimeMillis())
                .build(), null);

        // List all
        List<GatewaySecretInfo> all = gatewaySecretRepository.listSecrets(null);
        assertTrue(all.stream().anyMatch(s -> "list-secret-1".equals(s.getSecretId())));
        assertTrue(all.stream().anyMatch(s -> "list-secret-2".equals(s.getSecretId())));

        // Filter by provider
        List<GatewaySecretInfo> openaiSecrets = gatewaySecretRepository.listSecrets("openai");
        assertTrue(openaiSecrets.stream().anyMatch(s -> "list-secret-1".equals(s.getSecretId())));
        assertTrue(openaiSecrets.stream().noneMatch(s -> "list-secret-2".equals(s.getSecretId())));
    }

    @Test
    public void testDeleteSecret() throws IOException {
        gatewaySecretRepository.saveSecret(GatewaySecretInfo.builder()
                .secretId("delete-secret")
                .secretName("to-delete")
                .createdAt(System.currentTimeMillis())
                .lastUpdatedAt(System.currentTimeMillis())
                .build(), null);

        assertNotNull(gatewaySecretRepository.getSecretById("delete-secret"));

        gatewaySecretRepository.deleteSecret("delete-secret");

        assertNull(gatewaySecretRepository.getSecretById("delete-secret"));
    }

    @Test
    public void testGetNonExistentSecret() throws IOException {
        assertNull(gatewaySecretRepository.getSecretById("nonexistent-id"));
        assertNull(gatewaySecretRepository.getSecretByName("nonexistent-name"));
    }

    @Test
    public void testUpdateSecret() throws IOException {
        GatewaySecretInfo secret = GatewaySecretInfo.builder()
                .secretId("update-secret")
                .secretName("update-me")
                .provider("openai")
                .createdAt(1000L)
                .lastUpdatedAt(1000L)
                .createdBy("user1")
                .lastUpdatedBy("user1")
                .build();
        gatewaySecretRepository.saveSecret(secret, List.of(buildSecretValue("key", "old-value")));

        // Update
        secret.setLastUpdatedAt(2000L);
        secret.setLastUpdatedBy("user2");
        gatewaySecretRepository.saveSecret(secret, List.of(buildSecretValue("key", "new-value")));

        GatewaySecretInfo retrieved = gatewaySecretRepository.getSecretById("update-secret");
        assertEquals(2000L, retrieved.getLastUpdatedAt());
        assertEquals("user2", retrieved.getLastUpdatedBy());

        List<SecretValueEntry> values = gatewaySecretRepository.getSecretValues("update-secret");
        assertNotNull(values);
        assertEquals(1, values.size());
        assertEquals("new-value", values.get(0).getValue());
    }

    private MaskedValuesEntry buildMaskedValue(String key, String value) {
        MaskedValuesEntry entry = new MaskedValuesEntry();
        entry.setKey(key);
        entry.setValue(value);
        return entry;
    }

    private SecretValueEntry buildSecretValue(String key, String value) {
        SecretValueEntry entry = new SecretValueEntry();
        entry.setKey(key);
        entry.setValue(value);
        return entry;
    }

    private AuthConfigEntry buildAuthConfig(String key, String value) {
        AuthConfigEntry entry = new AuthConfigEntry();
        entry.setKey(key);
        entry.setValue(value);
        return entry;
    }
}
