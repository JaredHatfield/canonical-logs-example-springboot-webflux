package com.example.canonicallogs.logging;

import reactor.util.context.Context;

/**
 * Key for storing and retrieving {@link CanonicalLogContext} in Reactor Context.
 * 
 * <h2>Why Reactor Context?</h2>
 * <p>In reactive WebFlux applications, there is no thread-local storage that can reliably
 * hold request-scoped data because:
 * <ul>
 *   <li>A single request may execute across multiple threads</li>
 *   <li>A single thread may serve multiple requests concurrently</li>
 *   <li>Traditional MDC (Mapped Diagnostic Context) is thread-bound and doesn't work</li>
 * </ul>
 * 
 * <p>Reactor Context is an immutable, subscription-time propagated key-value store that
 * flows "upward" through the reactive chain from subscriber to publisher. This makes it
 * the idiomatic way to pass request-scoped data in WebFlux.
 * 
 * <h2>Important: Context is Immutable</h2>
 * <p>Reactor Context itself is immutable - you cannot add keys after creation. However,
 * you can store a <em>mutable object reference</em> (like {@link CanonicalLogContext})
 * in the Context, and that object can be mutated. This is the pattern used here:
 * <ul>
 *   <li>The Context key-value pair is immutable (set once at request start)</li>
 *   <li>The {@link CanonicalLogContext} value is mutable and accumulates data</li>
 * </ul>
 */
public final class CanonicalLogContextHolder {

    /**
     * The key used to store {@link CanonicalLogContext} in Reactor Context.
     */
    public static final Class<CanonicalLogContext> KEY = CanonicalLogContext.class;

    private CanonicalLogContextHolder() {
        // Utility class
    }

    /**
     * Creates a new Reactor Context containing a fresh {@link CanonicalLogContext}.
     * Typically called in a WebFilter at the start of request processing.
     */
    public static Context withNewContext(Context parent) {
        return parent.put(KEY, new CanonicalLogContext());
    }

    /**
     * Creates a new Reactor Context containing the given {@link CanonicalLogContext}.
     * Used when the context has already been created (e.g., by a WebFilter).
     */
    public static Context withContext(Context parent, CanonicalLogContext ctx) {
        return parent.put(KEY, ctx);
    }

    /**
     * Retrieves the {@link CanonicalLogContext} from Reactor Context.
     * 
     * @param context the Reactor Context
     * @return the CanonicalLogContext, or null if not present
     */
    public static CanonicalLogContext get(Context context) {
        return context.getOrDefault(KEY, null);
    }

    /**
     * Checks if a {@link CanonicalLogContext} is present in the Reactor Context.
     */
    public static boolean hasContext(Context context) {
        return context.hasKey(KEY);
    }
}
