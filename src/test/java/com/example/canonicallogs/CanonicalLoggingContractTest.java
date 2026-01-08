package com.example.canonicallogs;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests that validate the canonical logging contract:
 * - Exactly one log event per request
 * - Required fields exist in every log entry
 * - Error fields are captured correctly
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
class CanonicalLoggingContractTest {

    @Autowired
    private WebTestClient webTestClient;

    @Autowired
    private ObjectMapper objectMapper;

    private ListAppender<ILoggingEvent> listAppender;
    private Logger canonicalLogger;

    @BeforeEach
    void setUp() {
        // Get the canonical logger and attach a list appender
        canonicalLogger = (Logger) LoggerFactory.getLogger("canonical");
        listAppender = new ListAppender<>();
        listAppender.start();
        canonicalLogger.addAppender(listAppender);
    }

    @AfterEach
    void tearDown() {
        canonicalLogger.detachAppender(listAppender);
        listAppender.stop();
    }

    @Test
    void successfulRequest_emitsExactlyOneLogWithRequiredFields() throws Exception {
        // Clear any existing logs
        listAppender.list.clear();

        // Make a request
        webTestClient.post()
                .uri("/v1/turboencabulators/contract-test-123/runs")
                .header("X-User-Id", "contract-user")
                .exchange()
                .expectStatus().isOk();

        // Wait for async log emission using Awaitility
        await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(listAppender.list).hasSize(1));

        // Parse the log message as JSON
        String logMessage = listAppender.list.get(0).getFormattedMessage();
        Map<String, Object> logData = objectMapper.readValue(logMessage, new TypeReference<>() {});

        // Assert required fields exist
        assertThat(logData).containsKey("request_id");
        assertThat(logData).containsKey("http.method");
        assertThat(logData.get("http.method")).isEqualTo("POST");
        assertThat(logData).containsKey("http.target");
        assertThat(logData).containsKey("http.route");
        assertThat(logData.get("http.route")).isEqualTo("/v1/turboencabulators/{id}/runs");
        assertThat(logData).containsKey("http.status_code");
        assertThat(logData.get("http.status_code")).isEqualTo(200);
        assertThat(logData).containsKey("duration_ms");
        assertThat(logData).containsKey("outcome");
        assertThat(logData.get("outcome")).isEqualTo("success");
        assertThat(logData).containsKey("reactor.signal");
        assertThat(logData.get("reactor.signal")).isEqualTo("on_complete");

        // Assert service-specific fields were captured
        assertThat(logData).containsKey("turboencabulator.id");
        assertThat(logData.get("turboencabulator.id")).isEqualTo("contract-test-123");
        assertThat(logData).containsKey("user_id");
        assertThat(logData.get("user_id")).isEqualTo("contract-user");
    }

    @Test
    void multipleRequests_eachEmitsExactlyOneLog() throws Exception {
        listAppender.list.clear();

        // Make multiple requests
        for (int i = 0; i < 3; i++) {
            webTestClient.get()
                    .uri("/")
                    .exchange()
                    .expectStatus().isOk();
        }

        // Wait for async log emission using Awaitility
        await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(listAppender.list).hasSize(3));

        // Each log should have unique request_id
        List<String> requestIds = listAppender.list.stream()
                .map(event -> {
                    try {
                        Map<String, Object> data = objectMapper.readValue(
                                event.getFormattedMessage(), new TypeReference<>() {});
                        return (String) data.get("request_id");
                    } catch (Exception e) {
                        return null;
                    }
                })
                .toList();

        assertThat(requestIds).doesNotContainNull();
        assertThat(requestIds).doesNotHaveDuplicates();
    }

    @Test
    void notFoundRequest_emitsLogWithErrorStatus() throws Exception {
        listAppender.list.clear();

        // Make a request to a non-existent endpoint
        webTestClient.get()
                .uri("/nonexistent/endpoint")
                .exchange()
                .expectStatus().isNotFound();

        // Wait for async log emission using Awaitility
        await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(listAppender.list).hasSize(1));

        // Parse the log message
        String logMessage = listAppender.list.get(0).getFormattedMessage();
        Map<String, Object> logData = objectMapper.readValue(logMessage, new TypeReference<>() {});

        // Assert error status is captured
        assertThat(logData).containsKey("http.status_code");
        assertThat(logData.get("http.status_code")).isEqualTo(404);
        assertThat(logData).containsKey("outcome");
        assertThat(logData.get("outcome")).isEqualTo("failure");
    }

    @Test
    void requestWithDelay_emitsLogAfterCompletion() throws Exception {
        listAppender.list.clear();

        // Make a request to the churn endpoint (has built-in delay)
        webTestClient.post()
                .uri("/v1/turboencabulators/delay-test/churn")
                .exchange()
                .expectStatus().isOk();

        // Wait for the churn operation to complete and log to be emitted using Awaitility
        await().atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> assertThat(listAppender.list).hasSize(1));

        // Parse the log message
        String logMessage = listAppender.list.get(0).getFormattedMessage();
        Map<String, Object> logData = objectMapper.readValue(logMessage, new TypeReference<>() {});

        // Assert all required fields including churn-specific ones
        assertThat(logData).containsKey("request_id");
        assertThat(logData).containsKey("http.method");
        assertThat(logData).containsKey("duration_ms");
        assertThat(logData).containsKey("outcome");
        assertThat(logData).containsKey("churn_time");
        assertThat(logData).containsKey("churn_thread");

        // Duration should be at least as long as the churn time
        int durationMs = ((Number) logData.get("duration_ms")).intValue();
        int churnTime = ((Number) logData.get("churn_time")).intValue();
        assertThat(durationMs).isGreaterThanOrEqualTo(churnTime);
    }

    @Test
    void requestIdFromHeader_isUsedInLog() throws Exception {
        listAppender.list.clear();

        String customRequestId = "custom-request-id-12345";

        webTestClient.get()
                .uri("/")
                .header("X-Request-Id", customRequestId)
                .exchange()
                .expectStatus().isOk();

        // Wait for async log emission using Awaitility
        await().atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(listAppender.list).hasSize(1));

        String logMessage = listAppender.list.get(0).getFormattedMessage();
        Map<String, Object> logData = objectMapper.readValue(logMessage, new TypeReference<>() {});

        // Assert the custom request ID is used
        assertThat(logData.get("request_id")).isEqualTo(customRequestId);
    }
}
