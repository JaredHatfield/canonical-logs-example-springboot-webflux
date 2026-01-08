# canonical-logs-example-springboot-webflux

Example Spring Boot WebFlux application demonstrating canonical structured JSON logging in a reactive context.

## What is Canonical Logging?

This example demonstrates a production-ready approach to structured logging where:
- **Each HTTP request emits exactly one canonical structured JSON log line**
- That one log line contains:
  - Common request attributes (method, route template, status, duration, request id)
  - Common deployment attributes (service, env, region, version)
  - Controller-added attributes (domain identifiers from path or headers)
  - Service-added attributes (work details, timings, output summary)
- **Thread-safe for concurrent requests** using request-scoped state stored in exchange, optionally accessible via Reactor Context

## Key Concept: Reactor Context vs Request-Scoped Beans

In Spring MVC, the idiomatic way to share per-request state is a `@RequestScope` bean backed by thread-local storage (`RequestContextHolder`). In WebFlux, this doesn't work because:

1. **A single request may execute across multiple threads** - reactive operators can switch threads at any point
2. **A single thread may serve multiple requests concurrently** - the event loop handles many requests
3. **Thread-local storage is unreliable** - MDC and `RequestContextHolder` don't propagate through reactive chains

Instead, WebFlux uses **Reactor Context**, an immutable key-value store that propagates through the reactive chain.

## Architecture

### Core Components

1. **CanonicalLogContext** - Mutable holder for log data, passed through Reactor Context
2. **CanonicalLogContextHolder** - Utility for storing/retrieving context from Reactor Context
3. **CanonicalWebFilter** - WebFilter that initializes context and emits the final log
4. **CanonicalLogger** - Dedicated logger component

### Example Implementation

- **TurboEncabulatorController** - Shows how controllers add domain and request context
- **TurboEncabulatorService** - Shows how services add processing details via Reactor Context

#### Available Endpoints

1. **POST /v1/turboencabulators/{id}/runs** - Compute a turboencabulator run value
   - Demonstrates basic structured logging with reactive context
   
2. **POST /v1/turboencabulators/{id}/churn** - Execute an async churn task
   - Demonstrates reactive delay (`Mono.delay`) with context propagation
   - Unlike MVC, no manual thread-pool executor or `RequestContextHolder` propagation needed

## Sample Canonical Log Output

```json
{
    "ts_start": "2026-01-07T23:31:47.084272336Z",
    "service": "turboencabulator-service",
    "env": "prod",
    "region": "us-east-1",
    "version": "2026.01.07.1",
    "kind": "http",
    "request_id": "req_410e6f0a-b55a-4666-98c4-ab52a713d34f",
    "http.method": "POST",
    "http.target": "/v1/turboencabulators/turbo-123/runs",
    "http.route": "/v1/turboencabulators/{id}/runs",
    "turboencabulator.id": "turbo-123",
    "user_id": "user-456",
    "compute.strategy": "random",
    "compute.duration_ms": 0,
    "turboencabulator.value": 4.072070056910634,
    "ts": "2026-01-07T23:31:47.148807915Z",
    "duration_ms": 64,
    "http.status_code": 200,
    "outcome": "success"
}
```

## How It Works: Servlet (MVC) vs Reactive (WebFlux)

### MVC Approach (Thread-Local Based)

```
Request Thread
├── OncePerRequestFilter: Initialize context in RequestContextHolder
├── Controller: Get @RequestScope bean via proxy
├── Service: Get @RequestScope bean via ObjectProvider
├── Background Thread (if any): Manually propagate RequestAttributes
└── HandlerInterceptor.afterCompletion(): Emit log

Key characteristics:
- @RequestScope beans are proxies that look up thread-local storage
- Thread-bound: same thread serves one request at a time
- Background threads need manual RequestContextHolder propagation
```

### WebFlux Approach (Reactor Context Based)

```
Reactive Pipeline (may span multiple threads)
├── WebFilter: Create CanonicalLogContext, add to Reactor Context
├── Controller: Get context from ServerWebExchange attributes
├── Service: Get context via Mono.deferContextual()
├── Async operations: Context propagates automatically through chain
└── WebFilter.doFinally(): Emit log when chain completes

Key characteristics:
- No thread-local storage - context flows through reactive chain
- Single context object per request, stored in Reactor Context
- Automatic propagation through flatMap, defer, delay, etc.
```

## Understanding Reactor Context Immutability

Reactor Context is **immutable** - you cannot add or modify entries in an existing Context. When you call `ctx.put()`, it returns a **new Context** with the added entry. This is by design for thread safety and functional programming principles.

**Key insight**: Context is metadata propagation, not a shared mutable map. You can add values during the chain setup, but you're always creating new Context instances, not mutating shared state.

**The Solution for Canonical Logging**: Store a **mutable object reference** in the immutable Context:

```java
// WebFilter: Create mutable holder once
CanonicalLogContext ctx = new CanonicalLogContext();
return chain.filter(exchange)
    .contextWrite(context -> context.put(CanonicalLogContext.class, ctx));

// Service: Mutate the holder (the reference in Context is unchanged)
return Mono.deferContextual(contextView -> {
    CanonicalLogContext ctx = contextView.get(CanonicalLogContext.class);
    ctx.put("service.field", value);  // Mutating the object, not the Context
    return doWork();
});
```

## How Observation Differs from MDC and Request Attributes

| Feature | MDC (Servlet) | Request Attributes | Micrometer Observation |
|---------|--------------|-------------------|----------------------|
| **Scope** | Thread-local | Request-scoped bean | Observation scope (span) |
| **Propagation** | Manual to other threads | Manual via RequestContextHolder | Automatic in reactive chains |
| **Purpose** | Logging context | General request state | Metrics & tracing |
| **Cardinality** | High (any string) | High (any object) | Low (tags for metrics) |
| **Use case** | Add fields to all log lines | Share state across components | Record timing, tags, traces |

**When to use each:**
- **Canonical logging (this pattern)**: Custom structured log with high-cardinality business attributes
- **Observation**: Metrics (Prometheus), distributed tracing (Zipkin, Jaeger), standard observability
- **MDC**: Adding context to individual log lines (but unreliable in reactive code)

## Accessing CanonicalLogContext

### In Controllers (via ServerWebExchange)

```java
@PostMapping("/{id}/runs")
public Mono<Response> run(@PathVariable String id, ServerWebExchange exchange) {
    CanonicalLogContext logCtx = CanonicalWebFilter.getContext(exchange);
    logCtx.put("entity.id", id);
    return service.doWork(id);
}
```

### In Services (via Reactor Context)

```java
public Mono<Result> doWork(String id) {
    return Mono.deferContextual(contextView -> {
        CanonicalLogContext logCtx = CanonicalLogContextHolder.get(contextView);
        if (logCtx != null) {
            logCtx.put("work.detail", "computed");
        }
        return actualWork();
    });
}
```

### Alternative: Pass Context Explicitly

```java
// Controller
CanonicalLogContext ctx = CanonicalWebFilter.getContext(exchange);
return service.doWork(id, ctx);

// Service
public Mono<Result> doWork(String id, CanonicalLogContext ctx) {
    ctx.put("work.detail", "computed");
    return actualWork();
}
```

## Tradeoffs and Performance Considerations

### Benefits of This Pattern

1. **Observability** - Complete request context in one log line
2. **Searchability** - Structured JSON for log aggregation systems
3. **Traceability** - Request IDs link logs across services
4. **Thread Safety** - No race conditions from thread-local assumptions

### Performance Considerations

1. **Memory**: Each request allocates a `CanonicalLogContext` (~200 bytes + entries)
2. **Thread Safety**: The context uses `ConcurrentHashMap` for lock-free writes under load
3. **JSON Serialization**: One `ObjectMapper.writeValueAsString()` call per request (CPU work on completion thread)
4. **Context Propagation**: Reactor Context lookup has minimal overhead

### When NOT to Use This Pattern

- **High-frequency internal calls**: Consider sampling or aggregation
- **Extremely latency-sensitive paths**: The synchronization and JSON serialization add microseconds
- **Simple applications**: Standard logging may be sufficient

## Running the Application

```bash
mvn spring-boot:run
```

## Testing the Canonical Logging

### Basic Turboencabulator Run

```bash
# Make a request with user ID
curl -X POST http://localhost:8080/v1/turboencabulators/turbo-123/runs \
  -H "X-User-Id: user-456" \
  -H "Content-Type: application/json"

# Make a request without user ID
curl -X POST http://localhost:8080/v1/turboencabulators/turbo-789/runs \
  -H "Content-Type: application/json"
```

### Churn Endpoint (Async with Canonical Logging)

```bash
# Execute an async churn task with user ID
curl -X POST http://localhost:8080/v1/turboencabulators/turbo-999/churn \
  -H "X-User-Id: churn-user" \
  -H "Content-Type: application/json"

# Execute an async churn task without user ID
curl -X POST http://localhost:8080/v1/turboencabulators/turbo-888/churn \
  -H "Content-Type: application/json"
```

Check the application logs for a **single** canonical JSON log entry per request that includes:
- Standard HTTP request fields (method, path, status, duration, etc.)
- The `churn_time` field with the random sleep duration
- The `churn_thread` field showing which reactor thread handled the delay
- All in one unified log entry

## Running Tests

```bash
mvn test
```

## Configuration

Deployment attributes are configured in `src/main/resources/application.yml`:

```yaml
app:
  env: prod
  region: us-east-1
  version: 2026.01.07.1
```

## Benefits

1. **Observability** - One log line per request with complete context
2. **Searchability** - Structured JSON makes it easy to query and analyze
3. **Traceability** - Request IDs link logs across distributed services
4. **Performance** - Minimal overhead, emits exactly once
5. **Reactive-Safe** - Works correctly with WebFlux's non-blocking model
