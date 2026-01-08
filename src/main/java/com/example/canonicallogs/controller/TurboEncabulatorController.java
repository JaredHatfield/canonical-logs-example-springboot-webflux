package com.example.canonicallogs.controller;

import com.example.canonicallogs.logging.CanonicalLogContext;
import com.example.canonicallogs.logging.CanonicalLogContextHolder;
import com.example.canonicallogs.logging.CanonicalWebFilter;
import com.example.canonicallogs.service.TurboEncabulatorService;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Reactive controller demonstrating canonical log enrichment.
 * 
 * <h2>How to Access CanonicalLogContext in WebFlux</h2>
 * <p>There are two patterns for accessing the context:
 * 
 * <h3>1. Via ServerWebExchange (Controller Layer)</h3>
 * <p>In controllers, inject {@link ServerWebExchange} and use:
 * <pre>{@code
 * CanonicalLogContext ctx = CanonicalWebFilter.getContext(exchange);
 * ctx.put("my.field", value);
 * }</pre>
 * 
 * <h3>2. Via Reactor Context (Service Layer)</h3>
 * <p>In reactive operators, use {@code Mono.deferContextual}:
 * <pre>{@code
 * return Mono.deferContextual(contextView -> {
 *     CanonicalLogContext ctx = CanonicalLogContextHolder.get(Context.of(contextView));
 *     ctx.put("my.field", value);
 *     return doWork();
 * });
 * }</pre>
 * 
 * <h2>Key Differences from MVC</h2>
 * <ul>
 *   <li>No {@code @RequestScope} bean injection - context is explicitly accessed</li>
 *   <li>No thread-local storage - context flows through Reactor Context</li>
 *   <li>Context access is explicit, not implicit through proxies</li>
 * </ul>
 */
@RestController
@RequestMapping("/v1/turboencabulators")
public class TurboEncabulatorController {

    private final TurboEncabulatorService service;

    public TurboEncabulatorController(TurboEncabulatorService service) {
        this.service = service;
    }

    @PostMapping("/{id}/runs")
    public Mono<RunResponse> run(@PathVariable("id") String turboId,
                                 @RequestHeader(value = "X-User-Id", required = false) String userId,
                                 ServerWebExchange exchange) {

        CanonicalLogContext logCtx = CanonicalWebFilter.getContext(exchange);
        
        // Enrich context with controller-level attributes
        if (logCtx != null) {
            logCtx.put("turboencabulator.id", turboId);
            if (userId != null && !userId.isBlank()) {
                logCtx.put("user_id", userId);
            }
        }

        // Call service - which can further enrich the context via Reactor Context
        return service.computeRunValue(turboId)
                .map(RunResponse::new);
    }

    @PostMapping("/{id}/churn")
    public Mono<ChurnResponse> churn(@PathVariable("id") String turboId,
                                     @RequestHeader(value = "X-User-Id", required = false) String userId,
                                     ServerWebExchange exchange) {

        CanonicalLogContext logCtx = CanonicalWebFilter.getContext(exchange);
        
        if (logCtx != null) {
            logCtx.put("turboencabulator.id", turboId);
            if (userId != null && !userId.isBlank()) {
                logCtx.put("user_id", userId);
            }
        }

        // Perform async churn operation
        return service.performChurn(turboId)
                .thenReturn(new ChurnResponse("completed"));
    }

    public record RunResponse(double value) {}
    public record ChurnResponse(String status) {}
}
