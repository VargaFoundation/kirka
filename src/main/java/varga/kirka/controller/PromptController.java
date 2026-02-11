package varga.kirka.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import varga.kirka.model.Prompt;
import varga.kirka.service.PromptService;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/2.0/mlflow/prompts")
@RequiredArgsConstructor
public class PromptController {

    private final PromptService promptService;

    @lombok.Data
    public static class CreatePromptRequest {
        private String name;
        private String template;
        private String description;
        private Map<String, String> tags;
    }

    @lombok.Data
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class PromptResponse {
        private Prompt prompt;
    }

    @lombok.Data
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    public static class PromptsResponse {
        private List<Prompt> prompts;
    }

    @lombok.Data
    public static class DeletePromptRequest {
        private String id;
    }

    @PostMapping("/create")
    public PromptResponse createPrompt(@RequestBody CreatePromptRequest request) throws IOException {
        String name = request.getName();
        String template = request.getTemplate();
        String description = request.getDescription();
        Map<String, String> tags = request.getTags();

        log.info("REST request to create prompt: {}", name);
        Prompt prompt = promptService.createPrompt(name, template, description, tags);
        return new PromptResponse(prompt);
    }

    @GetMapping("/get")
    public PromptResponse getPrompt(@RequestParam("id") String id) throws IOException {
        Prompt prompt = promptService.getPrompt(id);
        return new PromptResponse(prompt);
    }

    @GetMapping("/list")
    public PromptsResponse listPrompts() throws IOException {
        List<Prompt> prompts = promptService.listPrompts();
        return new PromptsResponse(prompts);
    }

    @PostMapping("/delete")
    public Map<String, Object> deletePrompt(@RequestBody DeletePromptRequest request) throws IOException {
        promptService.deletePrompt(request.getId());
        return Map.of();
    }
}
