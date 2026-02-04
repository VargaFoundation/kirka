package varga.kirka.service;

import org.springframework.beans.factory.annotation.Autowired;
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
public class PromptService {

    private static final String RESOURCE_TYPE = "prompt";

    @Autowired
    private PromptRepository promptRepository;

    @Autowired
    private SecurityContextHelper securityContextHelper;

    public Prompt createPrompt(String name, String template, String description, Map<String, String> tags) throws IOException {
        String id = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        String currentUser = securityContextHelper.getCurrentUser();
        Prompt prompt = new Prompt();
        prompt.setId(id);
        prompt.setName(name);
        prompt.setVersion("1");
        prompt.setTemplate(template);
        prompt.setDescription(description);
        prompt.setCreationTimestamp(now);
        prompt.setLastUpdatedTimestamp(now);
        prompt.setTags(tags);
        prompt.setOwner(currentUser);
        promptRepository.createPrompt(prompt);
        return prompt;
    }

    public Prompt getPrompt(String id) throws IOException {
        Prompt prompt = promptRepository.getPrompt(id);
        if (prompt != null) {
            Map<String, String> tagsMap = prompt.getTags() != null ? prompt.getTags() : Map.of();
            securityContextHelper.checkReadAccess(RESOURCE_TYPE, id, prompt.getOwner(), tagsMap);
        }
        return prompt;
    }

    public List<Prompt> listPrompts() throws IOException {
        List<Prompt> prompts = promptRepository.listPrompts();
        // Filter prompts based on read access
        return prompts.stream()
                .filter(prompt -> {
                    Map<String, String> tagsMap = prompt.getTags() != null ? prompt.getTags() : Map.of();
                    return securityContextHelper.canRead(RESOURCE_TYPE, prompt.getId(), prompt.getOwner(), tagsMap);
                })
                .collect(Collectors.toList());
    }

    public void deletePrompt(String id) throws IOException {
        Prompt prompt = promptRepository.getPrompt(id);
        if (prompt != null) {
            Map<String, String> tagsMap = prompt.getTags() != null ? prompt.getTags() : Map.of();
            securityContextHelper.checkDeleteAccess(RESOURCE_TYPE, id, prompt.getOwner(), tagsMap);
        }
        promptRepository.deletePrompt(id);
    }
}
