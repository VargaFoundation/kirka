package varga.kirka.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import varga.kirka.model.AuthConfigEntry;
import varga.kirka.model.GatewaySecretInfo;
import varga.kirka.model.SecretValueEntry;
import varga.kirka.repo.GatewaySecretRepository;
import varga.kirka.security.SecurityContextHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class GatewaySecretServiceTest {

    @Mock
    private GatewaySecretRepository gatewaySecretRepository;

    @Mock
    private SecurityContextHelper securityContextHelper;

    @InjectMocks
    private GatewaySecretService gatewaySecretService;

    @BeforeEach
    void setUpAuthz() {
        when(securityContextHelper.getCurrentUser()).thenReturn("alice");
        when(securityContextHelper.tagsToMap(any(), any(), any())).thenReturn(Map.of());
        when(securityContextHelper.canRead(any(), any(), any(), any())).thenReturn(true);
    }

    private GatewaySecretInfo existingSecret(String id, String name) {
        return GatewaySecretInfo.builder()
                .secretId(id)
                .secretName(name)
                .provider("openai")
                .createdBy("alice")
                .createdAt(System.currentTimeMillis())
                .lastUpdatedAt(System.currentTimeMillis())
                .maskedValues(new ArrayList<>())
                .build();
    }

    @Test
    public void testCreateSecret() throws IOException {
        // The service records the caller from the security context; override the default stub
        // so the assertions describe the returned DTO explicitly.
        when(securityContextHelper.getCurrentUser()).thenReturn("user1");

        List<SecretValueEntry> values = List.of(
                SecretValueEntry.builder().key("api_key").value("secret123").build());

        GatewaySecretInfo result = gatewaySecretService.createSecret(
                "test-secret", values, "openai", null, "user1");

        assertNotNull(result);
        assertNotNull(result.getSecretId());
        assertEquals("test-secret", result.getSecretName());
        assertEquals("openai", result.getProvider());
        assertEquals("user1", result.getCreatedBy());
        assertEquals("user1", result.getLastUpdatedBy());
        assertNotNull(result.getMaskedValues());
        assertEquals(1, result.getMaskedValues().size());
        assertEquals("api_key", result.getMaskedValues().get(0).getKey());
        assertEquals("********", result.getMaskedValues().get(0).getValue());
        verify(gatewaySecretRepository).saveSecret(any(), any());
    }

    @Test
    public void testCreateSecretWithAuthConfig() throws IOException {
        List<SecretValueEntry> values = List.of(
                SecretValueEntry.builder().key("api_key").value("secret123").build());
        List<AuthConfigEntry> authConfig = List.of(
                AuthConfigEntry.builder().key("auth_type").value("bearer").build());

        GatewaySecretInfo result = gatewaySecretService.createSecret(
                "test-secret", values, "openai", authConfig, "user1");

        assertNotNull(result);
        assertNotNull(result.getAuthConfig());
        assertEquals(1, result.getAuthConfig().size());
    }

    @Test
    public void testCreateSecretWithNullValues() throws IOException {
        GatewaySecretInfo result = gatewaySecretService.createSecret(
                "test-secret", null, "openai", null, "user1");
        assertNotNull(result);
        assertTrue(result.getMaskedValues().isEmpty());
    }

    @Test
    public void testUpdateSecret() throws IOException {
        when(gatewaySecretRepository.getSecretById("secret-123")).thenReturn(existingSecret("secret-123", "test-secret"));

        List<SecretValueEntry> newValues = List.of(
                SecretValueEntry.builder().key("api_key").value("newsecret456").build(),
                SecretValueEntry.builder().key("api_secret").value("anothersecret").build());
        GatewaySecretInfo updated = gatewaySecretService.updateSecret("secret-123", newValues, null, "user2");

        assertNotNull(updated);
        assertEquals("user2", updated.getLastUpdatedBy());
        assertEquals(2, updated.getMaskedValues().size());
    }

    @Test
    public void testUpdateSecretAuthConfig() throws IOException {
        when(gatewaySecretRepository.getSecretById("secret-123")).thenReturn(existingSecret("secret-123", "test-secret"));
        when(gatewaySecretRepository.getSecretValues("secret-123")).thenReturn(null);

        List<AuthConfigEntry> authConfig = List.of(
                AuthConfigEntry.builder().key("auth_type").value("bearer").build());
        GatewaySecretInfo updated = gatewaySecretService.updateSecret("secret-123", null, authConfig, "user2");

        assertNotNull(updated);
        assertNotNull(updated.getAuthConfig());
        assertEquals(1, updated.getAuthConfig().size());
    }

    @Test
    public void testUpdateSecretNotFound() throws IOException {
        when(gatewaySecretRepository.getSecretById("nonexistent-id")).thenReturn(null);
        assertThrows(RuntimeException.class, () ->
                gatewaySecretService.updateSecret("nonexistent-id", null, null, "user1"));
    }

    @Test
    public void testDeleteSecret() throws IOException {
        when(gatewaySecretRepository.getSecretById("secret-123")).thenReturn(existingSecret("secret-123", "test-secret"));
        gatewaySecretService.deleteSecret("secret-123");
        verify(gatewaySecretRepository).deleteSecret("secret-123");
    }

    @Test
    public void testGetSecretById() throws IOException {
        when(gatewaySecretRepository.getSecretById("secret-123"))
                .thenReturn(existingSecret("secret-123", "test-secret"));
        GatewaySecretInfo result = gatewaySecretService.getSecret("secret-123", null);
        assertNotNull(result);
        assertEquals("secret-123", result.getSecretId());
    }

    @Test
    public void testGetSecretByName() throws IOException {
        when(gatewaySecretRepository.getSecretByName("unique-secret-name"))
                .thenReturn(existingSecret("secret-123", "unique-secret-name"));
        GatewaySecretInfo result = gatewaySecretService.getSecret(null, "unique-secret-name");
        assertNotNull(result);
        assertEquals("unique-secret-name", result.getSecretName());
    }

    @Test
    public void testGetSecretNotFound() throws IOException {
        when(gatewaySecretRepository.getSecretById("nonexistent-id")).thenReturn(null);
        when(gatewaySecretRepository.getSecretByName("nonexistent-name")).thenReturn(null);

        // After the stabilization pass the service raises a typed error instead of returning null.
        assertThrows(ResourceNotFoundException.class,
                () -> gatewaySecretService.getSecret("nonexistent-id", null));
        assertThrows(ResourceNotFoundException.class,
                () -> gatewaySecretService.getSecret(null, "nonexistent-name"));
    }

    @Test
    public void testListSecrets() throws IOException {
        List<GatewaySecretInfo> secrets = List.of(
                existingSecret("1", "secret1"),
                existingSecret("2", "secret2"),
                existingSecret("3", "secret3"));
        when(gatewaySecretRepository.listSecrets(null)).thenReturn(secrets);
        assertEquals(3, gatewaySecretService.listSecrets(null).size());
    }

    @Test
    public void testListSecretsFilterByProvider() throws IOException {
        List<GatewaySecretInfo> openaiSecrets = List.of(
                existingSecret("1", "secret1"),
                existingSecret("3", "secret3"));
        List<GatewaySecretInfo> anthropicSecrets = List.of(existingSecret("2", "secret2"));

        when(gatewaySecretRepository.listSecrets("openai")).thenReturn(openaiSecrets);
        when(gatewaySecretRepository.listSecrets("anthropic")).thenReturn(anthropicSecrets);

        assertEquals(2, gatewaySecretService.listSecrets("openai").size());
        assertEquals(1, gatewaySecretService.listSecrets("anthropic").size());
    }

    @Test
    public void testListSecretsEmpty() throws IOException {
        when(gatewaySecretRepository.listSecrets(null)).thenReturn(new ArrayList<>());
        assertTrue(gatewaySecretService.listSecrets(null).isEmpty());
    }
}
