package varga.kirka.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import varga.kirka.model.Prompt;
import varga.kirka.repo.PromptRepository;
import varga.kirka.security.SecurityContextHelper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class PromptService {

    private static final String RESOURCE_TYPE = "prompt";

    private final PromptRepository promptRepository;

    private final SecurityContextHelper securityContextHelper;

    public Prompt createPrompt(String name, String template, String description, Map<String, String> tags) throws IOException {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Prompt name must not be empty");
        }
        if (template == null || template.isBlank()) {
            throw new IllegalArgumentException("Prompt template must not be empty");
        }
        String id = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        String currentUser = securityContextHelper.getCurrentUser();
        Prompt prompt = Prompt.builder()
                .id(id)
                .name(name)
                .version("1")
                .template(template)
                .description(description)
                .creationTimestamp(now)
                .lastUpdatedTimestamp(now)
                .tags(tags)
                .owner(currentUser)
                .build();
        promptRepository.createPrompt(prompt);
        return prompt;
    }

    public Prompt getPrompt(String id) throws IOException {
        Prompt prompt = promptRepository.getPrompt(id);
        if (prompt == null) {
            throw new ResourceNotFoundException("Prompt", id);
        }
        Map<String, String> tagsMap = prompt.getTags() != null ? prompt.getTags() : Map.of();
        securityContextHelper.checkReadAccess(RESOURCE_TYPE, id, prompt.getOwner(), tagsMap);
        return prompt;
    }

    public List<Prompt> listPrompts() throws IOException {
        List<Prompt> prompts = promptRepository.listPrompts();
        return prompts.stream()
                .filter(prompt -> {
                    Map<String, String> tagsMap = prompt.getTags() != null ? prompt.getTags() : Map.of();
                    return securityContextHelper.canRead(RESOURCE_TYPE, prompt.getId(), prompt.getOwner(), tagsMap);
                })
                .collect(Collectors.toList());
    }

    public void deletePrompt(String id) throws IOException {
        Prompt prompt = promptRepository.getPrompt(id);
        if (prompt == null) {
            throw new ResourceNotFoundException("Prompt", id);
        }
        Map<String, String> tagsMap = prompt.getTags() != null ? prompt.getTags() : Map.of();
        securityContextHelper.checkDeleteAccess(RESOURCE_TYPE, id, prompt.getOwner(), tagsMap);
        promptRepository.deletePrompt(id);
    }
}
