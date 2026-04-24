package varga.kirka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@Slf4j
@SpringBootApplication
@EnableScheduling
public class KirkaApplication {
    public static void main(String[] args) {
        SpringApplication.run(KirkaApplication.class, args);
    }
}
