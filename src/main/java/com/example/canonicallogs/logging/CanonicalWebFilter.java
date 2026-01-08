package com.example.canonicallogs.logging;

import com.example.canonicallogs.config.AppRuntimeProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.core.Ordered;
import org.springframework.core.env.Environment;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * WebFlux filter that initializes canonical log context at request start
 * and emits the final log record at request completion.
 * 
 * <h2>How This Differs from Servlet Filters</h2>
 * <p>In Spring MVC, we used {@code OncePerRequestFilter} plus a {@code HandlerInterceptor}:
 * <ul>
 *   <li>Filter: Initialize context early in request lifecycle</li>
 *   <li>Interceptor: Emit log in {@code afterCompletion}</li>
 * </ul>
 * 
 * <p>In WebFlux, we use a single {@code WebFilter} that:
 * <ul>
 *   <li>Creates and initializes {@link CanonicalLogContext} at request start</li>
 *   <li>Places it in Reactor Context for downstream operators to access</li>
 *   <li>Uses {@code doOnError} to capture errors in the reactive chain</li>
 *   <li>Uses {@code doFinally} to emit the log when the reactive chain completes</li>
 * </ul>
 * 
 * <h2>Reactor Context Propagation</h2>
 * <p>The key insight is that Reactor Context propagates "upward" from subscriber to publisher.
 * By using {@code contextWrite} at the start of the operator chain, we make the context available
 * to all downstream operators. The {@code doFinally} callback executes when the request
 * completes (success, error, or cancel), ensuring exactly one log record per request.
 * 
 * <h2>Complementary Use with Micrometer Observation</h2>
 * <p>Micrometer Observation is designed for metrics and tracing, while this pattern is for
 * structured "fat" log records. You can use both: Observation for metrics/tracing and
 * this pattern for canonical logs. Correlate them via trace/request ids if tracing is enabled.
 * 
 * <h2>Performance Note</h2>
 * <p>JSON serialization (ObjectMapper.writeValueAsString) is CPU work performed on the request
 * completion thread. For most applications this is negligible, but for very high QPS endpoints,
 * consider benchmarking or offloading serialization to boundedElastic scheduler if needed.
 */
@Component
public class CanonicalWebFilter implements WebFilter, Ordered {

    private static final int MAX_ERROR_MESSAGE_LENGTH = 500;
    private static final String CANONICAL_CONTEXT_ATTR = "canonical.log.context";

    private final Environment env;
    private final AppRuntimeProperties runtime;
    private final ObjectMapper mapper;
    private final CanonicalLogger canonicalLogger;

    public CanonicalWebFilter(Environment env,
                              AppRuntimeProperties runtime,
                              ObjectMapper mapper,
                              CanonicalLogger canonicalLogger) {
        this.env = env;
        this.runtime = runtime;
        this.mapper = mapper;
        this.canonicalLogger = canonicalLogger;
    }

    @Override
    public int getOrder() {
        // Run early to capture timing
        return Ordered.HIGHEST_PRECEDENCE + 100;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        // Create the canonical log context for this request
        CanonicalLogContext ctx = new CanonicalLogContext();
        
        // Store in exchange attributes for controller/service access
        exchange.getAttributes().put(CANONICAL_CONTEXT_ATTR, ctx);
        
        // Initialize baseline fields
        initializeContext(ctx, exchange.getRequest());

        // Use an AtomicReference to capture status reliably across async boundaries.
        AtomicReference<Integer> capturedStatus = new AtomicReference<>();
        
        // Create a response decorator to capture status when it's set
        ServerWebExchange decoratedExchange = exchange.mutate()
                .response(new StatusCapturingResponse(exchange.getResponse(), capturedStatus))
                .build();

        // Also register beforeCommit as a fallback for status capture
        exchange.getResponse().beforeCommit(() -> {
            var statusCode = exchange.getResponse().getStatusCode();
            if (statusCode != null) {
                capturedStatus.compareAndSet(null, statusCode.value());
            }
            return Mono.empty();
        });

        // Process the request with proper reactive chain ordering:
        // contextWrite first (makes context available to downstream),
        // then doOnError (captures errors in the reactive chain),
        // then doFinally (emits log on completion/error/cancel)
        return chain.filter(decoratedExchange)
                .contextWrite(context -> CanonicalLogContextHolder.withContext(context, ctx))
                .doOnError(error -> {
                    ctx.put("outcome", "failure");
                    ctx.put("error_type", error.getClass().getSimpleName());
                    ctx.put("error_message", safeMessage(error.getMessage()));
                })
                .doFinally(signalType -> {
                    // Final status check - try response.getStatusCode() as fallback
                    var statusCode = exchange.getResponse().getStatusCode();
                    if (statusCode != null) {
                        capturedStatus.compareAndSet(null, statusCode.value());
                    }
                    emitCanonicalLog(ctx, exchange, capturedStatus.get(), signalType);
                });
    }

    /**
     * Response decorator that captures the status code when it's set.
     */
    private static class StatusCapturingResponse 
            extends org.springframework.http.server.reactive.ServerHttpResponseDecorator {
        
        private final AtomicReference<Integer> capturedStatus;
        
        StatusCapturingResponse(ServerHttpResponse delegate, AtomicReference<Integer> capturedStatus) {
            super(delegate);
            this.capturedStatus = capturedStatus;
        }
        
        @Override
        public boolean setStatusCode(org.springframework.http.HttpStatusCode status) {
            if (status != null) {
                capturedStatus.compareAndSet(null, status.value());
            }
            return super.setStatusCode(status);
        }
    }

    private void initializeContext(CanonicalLogContext ctx, ServerHttpRequest request) {
        String requestId = firstNonBlank(
                request.getHeaders().getFirst("X-Request-Id"),
                "req_" + UUID.randomUUID()
        );

        ctx.put("ts_start", ctx.start().toString());
        ctx.put("service", env.getProperty("spring.application.name", "unknown-service"));
        ctx.put("env", runtime.env());
        ctx.put("region", runtime.region());
        ctx.put("version", runtime.version());

        ctx.put("kind", "http");
        ctx.put("request_id", requestId);

        ctx.put("http.method", request.getMethod().name());
        ctx.put("http.target", request.getPath().value());
        
        // Note: In WebFlux, route pattern is not immediately available here.
        // It gets set later by the router/handler mapping.
    }

    private void emitCanonicalLog(CanonicalLogContext ctx, ServerWebExchange exchange, 
                                   Integer capturedStatus, SignalType signalType) {
        // Ensure exactly one emission per request
        if (!ctx.markEmittedIfFirst()) {
            return;
        }

        // Use monotonic nanoTime for accurate duration calculation (not affected by clock changes)
        long durationMs = (System.nanoTime() - ctx.startNano()) / 1_000_000;
        Instant end = Instant.now();

        ServerHttpResponse response = exchange.getResponse();
        Integer statusCode = capturedStatus;
        
        // Fall back to response.getStatusCode() if decorator didn't capture
        if (statusCode == null && response.getStatusCode() != null) {
            statusCode = response.getStatusCode().value();
        }

        // Handle different signal types appropriately
        if (signalType == SignalType.CANCEL) {
            // Handle cancellation explicitly - don't default to 200
            ctx.put("outcome", "cancel");
            ctx.put("reactor.signal", "cancel");
            // Only set status code if we actually observed one
            if (statusCode != null) {
                ctx.put("http.status_code", statusCode);
            }
        } else if (signalType == SignalType.ON_ERROR) {
            // Error case - outcome should already be set by doOnError
            ctx.put("reactor.signal", "on_error");
            if (statusCode != null) {
                ctx.put("http.status_code", statusCode);
            }
            // Ensure outcome is set even if doOnError didn't run (shouldn't happen, but defensive)
            if (ctx.snapshot().get("outcome") == null) {
                ctx.put("outcome", "failure");
            }
        } else {
            // ON_COMPLETE case - successful completion
            ctx.put("reactor.signal", signalType.name().toLowerCase());
            
            // For successful completion, default to 200 if no status was explicitly set.
            // This is standard WebFlux behavior - 200 is the implicit default for OK responses.
            if (statusCode == null) {
                statusCode = 200;
            }
            ctx.put("http.status_code", statusCode);
            
            // Set outcome based on status if not already set (e.g., by doOnError)
            if (ctx.snapshot().get("outcome") == null) {
                ctx.put("outcome", outcomeFromStatus(statusCode));
            }
        }

        // Try to get the matched route pattern
        Object pattern = exchange.getAttribute(
                org.springframework.web.reactive.HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE
        );
        if (pattern != null) {
            ctx.put("http.route", pattern.toString());
        }

        ctx.put("ts", end.toString());
        ctx.put("duration_ms", durationMs);

        try {
            Map<String, Object> payload = ctx.snapshot();
            canonicalLogger.info(mapper.writeValueAsString(payload));
        } catch (Exception emitError) {
            // Never break request completion because logging failed
            canonicalLogger.info("{\"kind\":\"http\",\"outcome\":\"failure\",\"error_type\":\"CanonicalEmitFailed\"}");
        }
    }

    private static String outcomeFromStatus(int status) {
        if (status >= 200 && status < 400) return "success";
        if (status == 408) return "timeout";
        if (status == 429) return "rejected";
        return "failure";
    }

    private static String safeMessage(String msg) {
        if (msg == null) return null;
        String t = msg.trim();
        return t.length() > MAX_ERROR_MESSAGE_LENGTH 
                ? t.substring(0, MAX_ERROR_MESSAGE_LENGTH) 
                : t;
    }

    private static String firstNonBlank(String a, String b) {
        return Optional.ofNullable(a).filter(s -> !s.isBlank()).orElse(b);
    }

    /**
     * Retrieves the {@link CanonicalLogContext} from the exchange attributes.
     * Use this in controllers and services that have access to {@link ServerWebExchange}.
     */
    public static CanonicalLogContext getContext(ServerWebExchange exchange) {
        return (CanonicalLogContext) exchange.getAttributes().get(CANONICAL_CONTEXT_ATTR);
    }
}
