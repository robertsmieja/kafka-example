package com.robertsmieja.example.kafka.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
public class ConsumerController {

    @GetMapping(value = "/consumer", produces = "text/event-stream")
    public Flux<String> consumer(){
        return Flux.just("hello consumer!");
//        Mono.just("hello consumer!");
    }
}
