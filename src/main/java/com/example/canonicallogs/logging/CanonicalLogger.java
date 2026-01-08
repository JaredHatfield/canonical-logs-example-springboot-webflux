package com.example.canonicallogs.logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Dedicated logger component for canonical log output.
 * 
 * <p>Separates canonical log output from regular application logs,
 * making it easier to route, filter, and process in log aggregation systems.
 */
@Component
public class CanonicalLogger {
    private final Logger log = LoggerFactory.getLogger("canonical");
    
    public void info(String json) {
        log.info(json);
    }
}
