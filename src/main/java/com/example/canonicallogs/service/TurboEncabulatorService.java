package com.example.canonicallogs.service;

import com.example.canonicallogs.logging.CanonicalLogContext;
import com.example.canonicallogs.logging.CanonicalLogContextHolder;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Reactive service demonstrating canonical log enrichment.
 * 
 * <h2>Accessing CanonicalLogContext in Services</h2>
 * <p>Services don't have direct access to {@link org.springframework.web.server.ServerWebExchange},
 * so they access the context via Reactor Context using {@code deferContextual}:
 * 
 * <pre>{@code
 * return Mono.deferContextual(contextView -> {
 *     CanonicalLogContext ctx = CanonicalLogContextHolder.get(contextView);
 *     if (ctx != null) {
 *         ctx.put("service.field", value);
 *     }
 *     return actualWork();
 * });
 * }</pre>
 * 
 * <h2>Why deferContextual?</h2>
 * <p>{@code Mono.deferContextual} defers the creation of the inner Mono until subscription time,
 * at which point the Reactor Context is available. This ensures we can read the context that
 * was written by the WebFilter upstream in the chain.
 * 
 * <h2>Contrast with MVC</h2>
 * <p>In MVC, services could simply inject {@code ObjectProvider<CanonicalLogContext>} and call
 * {@code getObject()} because Spring's request-scoped proxy handled the thread-local lookup.
 * In WebFlux, there's no thread-local storage, so we must explicitly propagate and read from
 * Reactor Context.
 * 
 * <h2>Alternative: Pass Context as Parameter</h2>
 * <p>For simpler cases, you can pass the context directly from controller to service:
 * <pre>{@code
 * // Controller
 * CanonicalLogContext ctx = CanonicalWebFilter.getContext(exchange);
 * return service.doWork(turboId, ctx);
 * 
 * // Service
 * public Mono<Result> doWork(String id, CanonicalLogContext ctx) {
 *     ctx.put("service.field", value);
 *     return ...;
 * }
 * }</pre>
 * <p>This is more explicit but less "magical". Choose based on your team's preferences.
 */
@Service
public class TurboEncabulatorService {

    /**
     * Computes a random value and enriches the canonical log with computation details.
     * Demonstrates accessing Reactor Context from a service method.
     */
    public Mono<Double> computeRunValue(String turboId) {
        return Mono.deferContextual(contextView -> {
            long startNs = System.nanoTime();
            
            // Example work: random number generator
            double value = ThreadLocalRandom.current().nextDouble(0.0, 10.0);
            
            long durationMs = (System.nanoTime() - startNs) / 1_000_000;

            // Access the canonical log context from Reactor Context using ContextView directly
            CanonicalLogContext logCtx = CanonicalLogContextHolder.get(contextView);
            if (logCtx != null) {
                logCtx.put("compute.strategy", "random");
                logCtx.put("compute.duration_ms", durationMs);
                logCtx.put("turboencabulator.value", value);
            }

            return Mono.just(value);
        });
    }

    /**
     * Performs an async "churn" operation with a random delay.
     * Demonstrates reactive delay and context enrichment.
     * 
     * <h3>Key Difference from MVC</h3>
     * <p>In MVC, the churn endpoint used a thread pool executor with manual
     * {@code RequestContextHolder} propagation. In WebFlux, we use reactive delay
     * ({@code Mono.delay}) which is non-blocking and context propagates automatically
     * through the reactive chain.
     */
    public Mono<Void> performChurn(String turboId) {
        // Generate random churn time (1-1000ms)
        int churnTimeMs = ThreadLocalRandom.current().nextInt(1, 1001);

        return Mono.deferContextual(contextView -> {
            // Access context before the delay to record intent
            CanonicalLogContext logCtx = CanonicalLogContextHolder.get(contextView);
            
            return Mono.delay(Duration.ofMillis(churnTimeMs))
                    .then(Mono.fromRunnable(() -> {
                        // After delay completes, enrich the context
                        if (logCtx != null) {
                            logCtx.put("churn_time", churnTimeMs);
                            logCtx.put("churn_thread", Thread.currentThread().getName());
                        }
                    }));
        });
    }
}
