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

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

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
 *   <li>Uses {@code doFinally} to emit the log when the reactive chain completes</li>
 * </ul>
 * 
 * <h2>Reactor Context Propagation</h2>
 * <p>The key insight is that Reactor Context propagates "upward" from subscriber to publisher.
 * By using {@code contextWrite} at the end of the filter chain, we make the context available
 * to all downstream operators. The {@code doFinally} callback executes when the request
 * completes (success, error, or cancel), ensuring exactly one log record per request.
 * 
 * <h2>Why Not Use Micrometer Observation?</h2>
 * <p>Micrometer Observation is designed for metrics and tracing, not for emitting structured
 * "fat" log records. While Observation provides hooks like {@code onStart}, {@code onStop},
 * and custom key-value pairs, it's optimized for:
 * <ul>
 *   <li>Low-cardinality tags suitable for metrics</li>
 *   <li>Trace context propagation (traceId, spanId)</li>
 *   <li>Standard observability backends (Prometheus, Zipkin, etc.)</li>
 * </ul>
 * 
 * <p>Our canonical log pattern requires:
 * <ul>
 *   <li>High-cardinality business attributes (user IDs, entity IDs, computed values)</li>
 *   <li>Custom JSON structure output</li>
 *   <li>Flexible attribute accumulation from controllers and services</li>
 * </ul>
 * 
 * <p>That said, you <em>can</em> combine both: use Observation for metrics/tracing and
 * this pattern for structured canonical logs. They serve complementary purposes.
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
        // Run early to capture timing, but after any security filters
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

        // Decorate the response to capture status code before completion
        ServerWebExchange decoratedExchange = exchange.mutate()
                .response(new StatusCapturingResponse(exchange.getResponse()))
                .build();

        // Process the request and emit log at completion
        return chain.filter(decoratedExchange)
                .doFinally(signalType -> emitCanonicalLog(ctx, decoratedExchange))
                .contextWrite(context -> CanonicalLogContextHolder.withContext(context, ctx));
    }

    /**
     * Response decorator that ensures status code is available for logging.
     */
    private static class StatusCapturingResponse 
            extends org.springframework.http.server.reactive.ServerHttpResponseDecorator {
        
        private Integer capturedStatus;
        
        StatusCapturingResponse(ServerHttpResponse delegate) {
            super(delegate);
        }
        
        @Override
        public boolean setStatusCode(org.springframework.http.HttpStatusCode status) {
            if (status != null) {
                this.capturedStatus = status.value();
            }
            return super.setStatusCode(status);
        }
        
        Integer getCapturedStatus() {
            // First try captured status, then delegate's status
            if (capturedStatus != null) {
                return capturedStatus;
            }
            org.springframework.http.HttpStatusCode status = getStatusCode();
            return status != null ? status.value() : null;
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

    private void emitCanonicalLog(CanonicalLogContext ctx, ServerWebExchange exchange) {
        // Ensure exactly one emission per request
        if (!ctx.markEmittedIfFirst()) {
            return;
        }

        Instant end = Instant.now();
        long durationMs = end.toEpochMilli() - ctx.start().toEpochMilli();

        ServerHttpResponse response = exchange.getResponse();
        Integer statusCode = null;
        
        // Try to get status from our capturing decorator first
        if (response instanceof StatusCapturingResponse capturingResponse) {
            statusCode = capturingResponse.getCapturedStatus();
        }
        // Fall back to standard method
        if (statusCode == null && response.getStatusCode() != null) {
            statusCode = response.getStatusCode().value();
        }
        // Default to 200 for successful completions with no explicit status
        if (statusCode == null) {
            statusCode = 200;
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
        ctx.put("http.status_code", statusCode);
        ctx.put("outcome", outcomeFromStatus(statusCode));

        // Check for error in exchange
        Throwable error = exchange.getAttribute(
                org.springframework.web.server.ServerWebExchange.LOG_ID_ATTRIBUTE + ".error"
        );
        if (error == null) {
            error = exchange.getAttribute("org.springframework.boot.web.reactive.error.DefaultErrorAttributes.ERROR");
        }
        
        if (error != null) {
            ctx.put("outcome", "failure");
            ctx.put("error_type", error.getClass().getSimpleName());
            ctx.put("error_message", safeMessage(error.getMessage()));
        }

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
