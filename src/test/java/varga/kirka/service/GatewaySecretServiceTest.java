package varga.kirka.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import varga.kirka.model.AuthConfigEntry;
import varga.kirka.model.GatewaySecretInfo;
import varga.kirka.model.SecretValueEntry;
import varga.kirka.repo.GatewaySecretRepository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class GatewaySecretServiceTest {

    @Mock
    private GatewaySecretRepository gatewaySecretRepository;

    @InjectMocks
    private GatewaySecretService gatewaySecretService;

    @BeforeEach
    public void setUp() {
    }

    @Test
    public void testCreateSecret() throws IOException {
        String name = "test-secret";
        List<SecretValueEntry> values = List.of(
                SecretValueEntry.builder().key("api_key").value("secret123").build()
        );
        String provider = "openai";
        String createdBy = "user1";

        doNothing().when(gatewaySecretRepository).saveSecret(any(), any());

        GatewaySecretInfo result = gatewaySecretService.createSecret(name, values, provider, null, createdBy);

        assertNotNull(result);
        assertNotNull(result.getSecretId());
        assertEquals(name, result.getSecretName());
        assertEquals(provider, result.getProvider());
        assertEquals(createdBy, result.getCreatedBy());
        assertEquals(createdBy, result.getLastUpdatedBy());
        assertNotNull(result.getMaskedValues());
        assertEquals(1, result.getMaskedValues().size());
        assertEquals("api_key", result.getMaskedValues().get(0).getKey());
        assertEquals("********", result.getMaskedValues().get(0).getValue());
        verify(gatewaySecretRepository).saveSecret(any(), any());
    }

    @Test
    public void testCreateSecretWithAuthConfig() throws IOException {
        String name = "test-secret";
        List<SecretValueEntry> values = List.of(
                SecretValueEntry.builder().key("api_key").value("secret123").build()
        );
        List<AuthConfigEntry> authConfig = List.of(
                AuthConfigEntry.builder().key("auth_type").value("bearer").build()
        );
        String provider = "openai";
        String createdBy = "user1";

        doNothing().when(gatewaySecretRepository).saveSecret(any(), any());

        GatewaySecretInfo result = gatewaySecretService.createSecret(name, values, provider, authConfig, createdBy);

        assertNotNull(result);
        assertNotNull(result.getAuthConfig());
        assertEquals(1, result.getAuthConfig().size());
    }

    @Test
    public void testCreateSecretWithNullValues() throws IOException {
        String name = "test-secret";
        String provider = "openai";
        String createdBy = "user1";

        doNothing().when(gatewaySecretRepository).saveSecret(any(), any());

        GatewaySecretInfo result = gatewaySecretService.createSecret(name, null, provider, null, createdBy);

        assertNotNull(result);
        assertTrue(result.getMaskedValues().isEmpty());
    }

    @Test
    public void testUpdateSecret() throws IOException {
        String secretId = "secret-123";
        GatewaySecretInfo existing = GatewaySecretInfo.builder()
                .secretId(secretId)
                .secretName("test-secret")
                .provider("openai")
                .createdBy("user1")
                .createdAt(System.currentTimeMillis())
                .lastUpdatedAt(System.currentTimeMillis())
                .maskedValues(new ArrayList<>())
                .build();

        when(gatewaySecretRepository.getSecretById(secretId)).thenReturn(existing);
        doNothing().when(gatewaySecretRepository).saveSecret(any(), any());

        List<SecretValueEntry> newValues = List.of(
                SecretValueEntry.builder().key("api_key").value("newsecret456").build(),
                SecretValueEntry.builder().key("api_secret").value("anothersecret").build()
        );
        GatewaySecretInfo updated = gatewaySecretService.updateSecret(secretId, newValues, null, "user2");

        assertNotNull(updated);
        assertEquals("user2", updated.getLastUpdatedBy());
        assertEquals(2, updated.getMaskedValues().size());
    }

    @Test
    public void testUpdateSecretAuthConfig() throws IOException {
        String secretId = "secret-123";
        GatewaySecretInfo existing = GatewaySecretInfo.builder()
                .secretId(secretId)
                .secretName("test-secret")
                .provider("openai")
                .createdBy("user1")
                .createdAt(System.currentTimeMillis())
                .lastUpdatedAt(System.currentTimeMillis())
                .maskedValues(new ArrayList<>())
                .build();

        when(gatewaySecretRepository.getSecretById(secretId)).thenReturn(existing);
        when(gatewaySecretRepository.getSecretValues(secretId)).thenReturn(null);
        doNothing().when(gatewaySecretRepository).saveSecret(any(), any());

        List<AuthConfigEntry> authConfig = List.of(
                AuthConfigEntry.builder().key("auth_type").value("bearer").build()
        );
        GatewaySecretInfo updated = gatewaySecretService.updateSecret(secretId, null, authConfig, "user2");

        assertNotNull(updated);
        assertNotNull(updated.getAuthConfig());
        assertEquals(1, updated.getAuthConfig().size());
    }

    @Test
    public void testUpdateSecretNotFound() throws IOException {
        when(gatewaySecretRepository.getSecretById("nonexistent-id")).thenReturn(null);

        assertThrows(RuntimeException.class, () -> {
            gatewaySecretService.updateSecret("nonexistent-id", null, null, "user1");
        });
    }

    @Test
    public void testDeleteSecret() throws IOException {
        doNothing().when(gatewaySecretRepository).deleteSecret(anyString());

        gatewaySecretService.deleteSecret("secret-123");

        verify(gatewaySecretRepository).deleteSecret("secret-123");
    }

    @Test
    public void testGetSecretById() throws IOException {
        String secretId = "secret-123";
        GatewaySecretInfo expected = GatewaySecretInfo.builder()
                .secretId(secretId)
                .secretName("test-secret")
                .build();

        when(gatewaySecretRepository.getSecretById(secretId)).thenReturn(expected);

        GatewaySecretInfo result = gatewaySecretService.getSecret(secretId, null);

        assertNotNull(result);
        assertEquals(secretId, result.getSecretId());
    }

    @Test
    public void testGetSecretByName() throws IOException {
        String name = "unique-secret-name";
        GatewaySecretInfo expected = GatewaySecretInfo.builder()
                .secretId("secret-123")
                .secretName(name)
                .build();

        when(gatewaySecretRepository.getSecretByName(name)).thenReturn(expected);

        GatewaySecretInfo result = gatewaySecretService.getSecret(null, name);

        assertNotNull(result);
        assertEquals(name, result.getSecretName());
    }

    @Test
    public void testGetSecretNotFound() throws IOException {
        when(gatewaySecretRepository.getSecretById("nonexistent-id")).thenReturn(null);
        when(gatewaySecretRepository.getSecretByName("nonexistent-name")).thenReturn(null);

        GatewaySecretInfo result = gatewaySecretService.getSecret("nonexistent-id", null);
        assertNull(result);

        result = gatewaySecretService.getSecret(null, "nonexistent-name");
        assertNull(result);
    }

    @Test
    public void testListSecrets() throws IOException {
        List<GatewaySecretInfo> secrets = List.of(
                GatewaySecretInfo.builder().secretId("1").secretName("secret1").provider("openai").build(),
                GatewaySecretInfo.builder().secretId("2").secretName("secret2").provider("anthropic").build(),
                GatewaySecretInfo.builder().secretId("3").secretName("secret3").provider("openai").build()
        );

        when(gatewaySecretRepository.listSecrets(null)).thenReturn(secrets);

        List<GatewaySecretInfo> allSecrets = gatewaySecretService.listSecrets(null);
        assertEquals(3, allSecrets.size());
    }

    @Test
    public void testListSecretsFilterByProvider() throws IOException {
        List<GatewaySecretInfo> openaiSecrets = List.of(
                GatewaySecretInfo.builder().secretId("1").secretName("secret1").provider("openai").build(),
                GatewaySecretInfo.builder().secretId("3").secretName("secret3").provider("openai").build()
        );
        List<GatewaySecretInfo> anthropicSecrets = List.of(
                GatewaySecretInfo.builder().secretId("2").secretName("secret2").provider("anthropic").build()
        );

        when(gatewaySecretRepository.listSecrets("openai")).thenReturn(openaiSecrets);
        when(gatewaySecretRepository.listSecrets("anthropic")).thenReturn(anthropicSecrets);

        List<GatewaySecretInfo> resultOpenai = gatewaySecretService.listSecrets("openai");
        assertEquals(2, resultOpenai.size());

        List<GatewaySecretInfo> resultAnthropic = gatewaySecretService.listSecrets("anthropic");
        assertEquals(1, resultAnthropic.size());
    }

    @Test
    public void testListSecretsEmpty() throws IOException {
        when(gatewaySecretRepository.listSecrets(null)).thenReturn(new ArrayList<>());

        List<GatewaySecretInfo> results = gatewaySecretService.listSecrets(null);
        assertTrue(results.isEmpty());
    }
}
