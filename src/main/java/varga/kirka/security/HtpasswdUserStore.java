package varga.kirka.security;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Loads Basic-auth credentials from an htpasswd-style file and verifies passwords with bcrypt.
 *
 * <p>Expected file format (one user per line):
 * <pre>
 *   alice:$2a$10$abcdef...        # bcrypt hash
 *   bob:$2y$10$xyz...
 * </pre>
 * Empty lines and lines starting with {@code #} are ignored. Any entry whose password hash does
 * not start with a supported bcrypt prefix is skipped with a warning, which prevents accidental
 * acceptance of plain-text or MD5 ({@code $apr1$}) entries that htpasswd can also produce.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "security.enabled", havingValue = "true")
public class HtpasswdUserStore {

    private final PasswordEncoder encoder = new BCryptPasswordEncoder();
    private final Map<String, String> hashes = new HashMap<>();

    @Value("${security.users.file:}")
    private String usersFile;

    @PostConstruct
    void load() {
        if (usersFile == null || usersFile.isBlank()) {
            log.warn("security.users.file is not configured; Basic auth will refuse every request. "
                    + "Provide a bcrypt htpasswd file or disable Basic auth.");
            return;
        }
        Path path = Path.of(usersFile);
        if (!Files.isRegularFile(path)) {
            log.error("security.users.file={} does not exist or is not a regular file", usersFile);
            return;
        }
        try {
            for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) continue;
                int colon = trimmed.indexOf(':');
                if (colon <= 0 || colon == trimmed.length() - 1) {
                    log.warn("Ignoring malformed line in {}: missing colon", usersFile);
                    continue;
                }
                String user = trimmed.substring(0, colon);
                String hash = trimmed.substring(colon + 1);
                if (!(hash.startsWith("$2a$") || hash.startsWith("$2b$") || hash.startsWith("$2y$"))) {
                    log.warn("Ignoring user {} with unsupported hash prefix; only bcrypt ($2a/$2b/$2y) is accepted", user);
                    continue;
                }
                hashes.put(user, hash);
            }
            log.info("Loaded {} user(s) from {}", hashes.size(), usersFile);
        } catch (IOException e) {
            log.error("Failed to read security.users.file={}", usersFile, e);
        }
    }

    /**
     * @return true when the supplied password matches the stored bcrypt hash for the user.
     *         Returns false for unknown users and for null/empty passwords.
     */
    public boolean verify(String username, String password) {
        if (username == null || password == null || password.isEmpty()) return false;
        String hash = hashes.get(username);
        if (hash == null) return false;
        return encoder.matches(password, hash);
    }
}
