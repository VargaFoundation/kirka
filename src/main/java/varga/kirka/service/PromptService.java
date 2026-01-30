package varga.kirka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import varga.kirka.model.Prompt;
import varga.kirka.repo.PromptRepository;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class PromptService {

    @Autowired
    private PromptRepository promptRepository;

    public Prompt createPrompt(String name, String template, String description, Map<String, String> tags) throws IOException {
        String id = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        Prompt prompt = new Prompt();
        /*
        prompt.setId(id);
        prompt.setName(name);
        prompt.setVersion("1");
        prompt.setTemplate(template);
        prompt.setDescription(description);
        prompt.setCreationTimestamp(now);
        prompt.setLastUpdatedTimestamp(now);
        prompt.setTags(tags);
        */
        promptRepository.createPrompt(prompt);
        return prompt;
    }

    public Prompt getPrompt(String id) throws IOException {
        return promptRepository.getPrompt(id);
    }

    public List<Prompt> listPrompts() throws IOException {
        return promptRepository.listPrompts();
    }

    public void deletePrompt(String id) throws IOException {
        promptRepository.deletePrompt(id);
    }
}
