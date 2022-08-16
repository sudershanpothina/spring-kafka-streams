package com.kafka.stream;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {
    @GetMapping("/health")
    public String getHealth() {
        return "healthly";
    }
}
