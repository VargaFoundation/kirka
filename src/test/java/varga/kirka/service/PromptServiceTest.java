package varga.kirka.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import varga.kirka.model.Prompt;
import varga.kirka.repo.PromptRepository;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class PromptServiceTest {

    @Mock
    private PromptRepository promptRepository;

    @InjectMocks
    private PromptService promptService;

    @Test
    public void testCreatePrompt() throws IOException {
        String name = "test-prompt";
        String template = "Hello {{name}}";
        String description = "A test prompt";
        Map<String, String> tags = new HashMap<>();
        tags.put("env", "test");

        Prompt result = promptService.createPrompt(name, template, description, tags);

        assertNotNull(result);
        verify(promptRepository, times(1)).createPrompt(any(Prompt.class));
    }

    @Test
    public void testGetPrompt() throws IOException {
        String id = "prompt123";
        Prompt prompt = Prompt.builder()
                .id(id)
                .name("test-prompt")
                .template("Hello {{name}}")
                .build();
        when(promptRepository.getPrompt(id)).thenReturn(prompt);

        Prompt result = promptService.getPrompt(id);

        assertNotNull(result);
        assertEquals(id, result.getId());
        assertEquals("test-prompt", result.getName());
        verify(promptRepository).getPrompt(id);
    }

    @Test
    public void testGetPromptNotFound() throws IOException {
        String id = "nonexistent";
        when(promptRepository.getPrompt(id)).thenReturn(null);

        Prompt result = promptService.getPrompt(id);

        assertNull(result);
        verify(promptRepository).getPrompt(id);
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

        List<Prompt> results = promptService.listPrompts();

        assertTrue(results.isEmpty());
        verify(promptRepository).listPrompts();
    }

    @Test
    public void testDeletePrompt() throws IOException {
        String id = "prompt123";

        promptService.deletePrompt(id);

        verify(promptRepository).deletePrompt(id);
    }
}
