package varga.kirka.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import varga.kirka.model.Prompt;
import varga.kirka.service.PromptService;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(PromptController.class)
public class PromptControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private PromptService promptService;

    @Test
    public void testCreatePrompt() throws Exception {
        Prompt prompt = Prompt.builder()
                .id("123")
                .name("test-prompt")
                .template("Hello {{name}}")
                .build();
        
        when(promptService.createPrompt(anyString(), anyString(), any(), any())).thenReturn(prompt);

        mockMvc.perform(post("/api/2.0/mlflow/prompts/create")
                .content("{\"name\": \"test-prompt\", \"template\": \"Hello {{name}}\"}")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.prompt.name").value("test-prompt"))
                .andExpect(jsonPath("$.prompt.template").value("Hello {{name}}"));
    }

    @Test
    public void testListPrompts() throws Exception {
        when(promptService.listPrompts()).thenReturn(List.of());

        mockMvc.perform(get("/api/2.0/mlflow/prompts/list")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.prompts").isArray());
    }

    @Test
    public void testGetPrompt() throws Exception {
        Prompt prompt = Prompt.builder()
                .id("123")
                .name("test-prompt")
                .template("Hello {{name}}")
                .build();
        
        when(promptService.getPrompt("123")).thenReturn(prompt);

        mockMvc.perform(get("/api/2.0/mlflow/prompts/get")
                .param("id", "123")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.prompt.id").value("123"))
                .andExpect(jsonPath("$.prompt.name").value("test-prompt"));
    }

    @Test
    public void testDeletePrompt() throws Exception {
        mockMvc.perform(post("/api/2.0/mlflow/prompts/delete")
                .content("{\"id\": \"123\"}")
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());
    }
}
