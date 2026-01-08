package com.example.canonicallogs.logging;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Mutable holder for canonical log data during a single request.
 * 
 * <p>In WebFlux, this object is created once per request and passed through the
 * Reactor pipeline via Reactor Context. Unlike the MVC version which uses 
 * {@code @RequestScope}, this class is a plain Java object that is explicitly 
 * propagated through the reactive chain.
 * 
 * <h2>Key Differences from MVC {@code @RequestScope}</h2>
 * <ul>
 *   <li>Not a Spring-managed bean - created per-request and passed via Context</li>
 *   <li>No thread-local dependency - safe for reactive, non-blocking execution</li>
 *   <li>Explicitly propagated, not implicitly available via proxy</li>
 * </ul>
 * 
 * <h2>Thread Safety</h2>
 * <p>Uses a synchronized map because multiple reactive operators may execute
 * concurrently or on different threads during a single request's lifecycle.
 */
public class CanonicalLogContext {

    private final Instant start = Instant.now();
    
    // Synchronized to handle concurrent access from multiple reactive operators
    private final Map<String, Object> fields = new LinkedHashMap<>();
    private final Object lock = new Object();
    
    private final AtomicBoolean emitted = new AtomicBoolean(false);

    public Instant start() {
        return start;
    }

    /**
     * Adds a key-value pair to the canonical log context.
     * Silently ignores null values to prevent polluting logs with null entries.
     *
     * @param key the field name (must not be null)
     * @param value the field value (ignored if null)
     */
    public void put(String key, Object value) {
        if (value != null) {
            synchronized (lock) {
                fields.put(key, value);
            }
        }
    }

    /**
     * Returns a snapshot of the current log fields.
     * Safe to call while other operators are still adding fields.
     */
    public Map<String, Object> snapshot() {
        synchronized (lock) {
            return new LinkedHashMap<>(fields);
        }
    }

    /**
     * Atomically marks this context as emitted and returns true if this was the first call.
     * Ensures exactly one canonical log record is emitted per request.
     */
    public boolean markEmittedIfFirst() {
        return emitted.compareAndSet(false, true);
    }
}
