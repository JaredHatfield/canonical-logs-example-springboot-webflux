package com.example.canonicallogs.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app")
public record AppRuntimeProperties(
    String env,
    String region,
    String version
) {}
