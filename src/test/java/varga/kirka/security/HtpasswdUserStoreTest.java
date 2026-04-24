package varga.kirka.security;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HtpasswdUserStoreTest {

    @Test
    void verifiesBcryptPasswordsAndRejectsUnknownUsers(@TempDir Path tmp) throws IOException {
        BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();
        Path htpasswd = tmp.resolve("users.htpasswd");
        Files.writeString(htpasswd,
                "# sample header\n"
                + "alice:" + encoder.encode("alice-secret") + "\n"
                + "\n"
                + "bob:" + encoder.encode("bob-secret") + "\n");

        HtpasswdUserStore store = newStore(htpasswd);

        assertTrue(store.verify("alice", "alice-secret"));
        assertTrue(store.verify("bob", "bob-secret"));
        assertFalse(store.verify("alice", "wrong"));
        assertFalse(store.verify("alice", ""));
        assertFalse(store.verify("alice", null));
        assertFalse(store.verify("unknown", "anything"));
        assertFalse(store.verify(null, "secret"));
    }

    @Test
    void ignoresNonBcryptHashes(@TempDir Path tmp) throws IOException {
        Path htpasswd = tmp.resolve("users.htpasswd");
        Files.writeString(htpasswd,
                "carol:$apr1$abcd$notbcrypt\n"
                + "dave:plaintextpwd\n");

        HtpasswdUserStore store = newStore(htpasswd);

        // Neither entry should authenticate because neither hash is bcrypt.
        assertFalse(store.verify("carol", "notbcrypt"));
        assertFalse(store.verify("dave", "plaintextpwd"));
    }

    @Test
    void handlesMissingFileGracefully(@TempDir Path tmp) {
        HtpasswdUserStore store = newStore(tmp.resolve("does-not-exist.htpasswd"));
        assertFalse(store.verify("alice", "whatever"));
    }

    @Test
    void handlesEmptyConfigurationPath() {
        HtpasswdUserStore store = new HtpasswdUserStore();
        ReflectionTestUtils.setField(store, "usersFile", "");
        store.load();
        assertFalse(store.verify("alice", "whatever"));
    }

    private static HtpasswdUserStore newStore(Path htpasswd) {
        HtpasswdUserStore store = new HtpasswdUserStore();
        ReflectionTestUtils.setField(store, "usersFile", htpasswd.toString());
        store.load();
        return store;
    }
}
