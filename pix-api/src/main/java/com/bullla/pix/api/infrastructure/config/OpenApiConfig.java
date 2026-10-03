package com.bullla.pix.api.infrastructure.config;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;

import org.springframework.context.annotation.Configuration;

@Configuration
@OpenAPIDefinition(info = @Info(
        title = "PIX API",
        version = "v1",
        description = "API for creating and querying PIX transactions. Processing is asynchronous."))
public class OpenApiConfig {
}
