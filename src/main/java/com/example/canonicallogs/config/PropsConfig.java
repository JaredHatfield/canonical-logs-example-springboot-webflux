package com.example.canonicallogs.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(AppRuntimeProperties.class)
class PropsConfig {}
