package varga.kirka.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import varga.kirka.model.Prompt;
import varga.kirka.repo.PromptRepository;
import varga.kirka.security.SecurityContextHelper;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class PromptServiceTest {

    @Mock
    private PromptRepository promptRepository;

    @Mock
    private SecurityContextHelper securityContextHelper;

    @InjectMocks
    private PromptService promptService;

    @BeforeEach
    void setUpAuthz() {
        when(securityContextHelper.getCurrentUser()).thenReturn("alice");
        when(securityContextHelper.tagsToMap(any(), any(), any())).thenReturn(Map.of());
        when(securityContextHelper.canRead(any(), any(), any(), any())).thenReturn(true);
    }

    @Test
    public void testCreatePrompt() throws IOException {
        Map<String, String> tags = new HashMap<>();
        tags.put("env", "test");

        Prompt result = promptService.createPrompt("test-prompt", "Hello {{name}}", "A test prompt", tags);

        assertNotNull(result);
        verify(promptRepository, times(1)).createPrompt(any(Prompt.class));
    }

    @Test
    public void testGetPrompt() throws IOException {
        Prompt prompt = Prompt.builder()
                .id("prompt123")
                .name("test-prompt")
                .template("Hello {{name}}")
                .build();
        when(promptRepository.getPrompt("prompt123")).thenReturn(prompt);

        Prompt result = promptService.getPrompt("prompt123");

        assertNotNull(result);
        assertEquals("prompt123", result.getId());
        assertEquals("test-prompt", result.getName());
        verify(promptRepository).getPrompt("prompt123");
    }

    @Test
    public void testGetPromptNotFound() throws IOException {
        when(promptRepository.getPrompt("nonexistent")).thenReturn(null);
        assertThrows(ResourceNotFoundException.class,
                () -> promptService.getPrompt("nonexistent"));
    }

    @Test
    public void testListPrompts() throws IOException {
        Prompt prompt1 = Prompt.builder().id("1").name("prompt1").build();
        Prompt prompt2 = Prompt.builder().id("2").name("prompt2").build();
        when(promptRepository.listPrompts()).thenReturn(List.of(prompt1, prompt2));

        List<Prompt> results = promptService.listPrompts();

        assertEquals(2, results.size());
        verify(promptRepository).listPrompts();
    }

    @Test
    public void testListPromptsEmpty() throws IOException {
        when(promptRepository.listPrompts()).thenReturn(Collections.emptyList());
        assertTrue(promptService.listPrompts().isEmpty());
        verify(promptRepository).listPrompts();
    }

    @Test
    public void testDeletePrompt() throws IOException {
        when(promptRepository.getPrompt("prompt123"))
                .thenReturn(Prompt.builder().id("prompt123").name("p").build());
        promptService.deletePrompt("prompt123");
        verify(promptRepository).deletePrompt("prompt123");
    }
}
