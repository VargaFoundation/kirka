package varga.kirka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class KirkaApplication {
    public static void main(String[] args) {
        log.info("Starting Kirka Application...");
        SpringApplication.run(KirkaApplication.class, args);
    }
}
