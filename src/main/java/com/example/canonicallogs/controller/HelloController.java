package com.example.canonicallogs.controller;

import com.example.canonicallogs.logging.CanonicalLogContext;
import com.example.canonicallogs.logging.CanonicalWebFilter;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@RestController
public class HelloController {

    @GetMapping("/")
    public Mono<String> hello(ServerWebExchange exchange) {
        CanonicalLogContext logCtx = CanonicalWebFilter.getContext(exchange);
        if (logCtx != null) {
            logCtx.put("endpoint", "hello");
        }
        return Mono.just("Hello World!");
    }
}
