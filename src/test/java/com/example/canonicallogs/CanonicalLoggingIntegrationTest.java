package com.example.canonicallogs;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
class CanonicalLoggingIntegrationTest {

    @Autowired
    private WebTestClient webTestClient;

    @Test
    void turboEncabulatorEndpointReturnsValidResponse() {
        webTestClient.post()
                .uri("/v1/turboencabulators/test-123/runs")
                .header("X-User-Id", "test-user")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.value").isNumber();
    }

    @Test
    void turboEncabulatorEndpointWorksWithoutUserId() {
        webTestClient.post()
                .uri("/v1/turboencabulators/test-456/runs")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.value").isNumber();
    }

    @Test
    void churnEndpointReturnsCompletedStatus() {
        webTestClient.post()
                .uri("/v1/turboencabulators/test-789/churn")
                .header("X-User-Id", "churn-user")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("completed");
    }

    @Test
    void churnEndpointWorksWithoutUserId() {
        webTestClient.post()
                .uri("/v1/turboencabulators/test-999/churn")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("completed");
    }

    @Test
    void helloControllerStillWorks() {
        webTestClient.get()
                .uri("/")
                .exchange()
                .expectStatus().isOk();
    }
}
