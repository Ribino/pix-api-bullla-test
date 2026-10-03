package com.bullla.pix.worker.support;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class PartnerMockProcess implements AutoCloseable {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Process process;
    private final String baseUrl;

    private PartnerMockProcess(Process process, String baseUrl) {
        this.process = process;
        this.baseUrl = baseUrl;
    }

    public static PartnerMockProcess start(int port, String scenario, long latencyMs) throws Exception {
        Path jar = resolveJar();
        String javaBinary = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
        ProcessBuilder builder = new ProcessBuilder(
                javaBinary, "-jar", jar.toString(), "--server.port=" + port);
        Map<String, String> environment = builder.environment();
        environment.put("PARTNER_MOCK_SCENARIO", scenario);
        environment.put("PARTNER_MOCK_LATENCY_MS", String.valueOf(latencyMs));
        builder.redirectOutput(ProcessBuilder.Redirect.DISCARD);
        builder.redirectError(ProcessBuilder.Redirect.DISCARD);
        Process process = builder.start();
        String baseUrl = "http://localhost:" + port;
        awaitReady(baseUrl);
        return new PartnerMockProcess(process, baseUrl);
    }

    public String baseUrl() {
        return baseUrl;
    }

    public void scenario(String mode, long latencyMs, int failTimes, long slowDelayMs) throws Exception {
        Map<String, Object> body = new HashMap<>();
        body.put("mode", mode);
        body.put("latencyMs", latencyMs);
        body.put("failTimes", failTimes);
        body.put("slowDelayMs", slowDelayMs);
        post("/partner/admin/scenario", OBJECT_MAPPER.writeValueAsString(body));
        post("/partner/admin/reset", "{}");
    }

    public MockStats stats() throws Exception {
        String body = get("/partner/admin/stats");
        Map<String, Object> parsed = OBJECT_MAPPER.readValue(body, Map.class);
        @SuppressWarnings("unchecked")
        List<String> ids = (List<String>) parsed.get("processedTransactionIds");
        return new MockStats((int) parsed.get("requestCount"), (int) parsed.get("processedCount"), ids);
    }

    @Override
    public void close() {
        process.destroy();
        try {
            if (!process.waitFor(15, TimeUnit.SECONDS)) {
                process.destroyForcibly();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }

    private static Path resolveJar() throws IOException {
        Path target = Paths.get("../pix-partner-mock/target").toAbsolutePath().normalize();
        try (Stream<Path> jars = Files.list(target)) {
            return jars.filter(path -> path.getFileName().toString().startsWith("pix-partner-mock-"))
                    .filter(path -> path.getFileName().toString().endsWith(".jar"))
                    .filter(path -> !path.getFileName().toString().endsWith(".original"))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "Partner mock jar not found in " + target + ". Run the build first."));
        }
    }

    private static void awaitReady(String baseUrl) throws Exception {
        Instant deadline = Instant.now().plus(Duration.ofSeconds(90));
        while (Instant.now().isBefore(deadline)) {
            if (isReachable(baseUrl)) {
                return;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("Partner mock did not start at " + baseUrl);
    }

    private static boolean isReachable(String baseUrl) {
        try {
            java.net.URI uri = java.net.URI.create(baseUrl + "/partner/admin/stats");
            java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();
            java.net.http.HttpRequest request = java.net.http.HttpRequest.newBuilder(uri)
                    .timeout(Duration.ofSeconds(2))
                    .GET()
                    .build();
            return client.send(request, java.net.http.HttpResponse.BodyHandlers.discarding())
                    .statusCode() == 200;
        } catch (Exception ignored) {
            return false;
        }
    }

    private String post(String path, String body) throws Exception {
        java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();
        java.net.http.HttpRequest request = java.net.http.HttpRequest
                .newBuilder(java.net.URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body))
                .build();
        java.net.http.HttpResponse<String> response = client.send(request,
                java.net.http.HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() >= 400) {
            throw new IllegalStateException("Admin call failed: " + path + " -> " + response.statusCode());
        }
        return response.body();
    }

    private String get(String path) throws Exception {
        java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();
        java.net.http.HttpRequest request = java.net.http.HttpRequest
                .newBuilder(java.net.URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        java.net.http.HttpResponse<String> response = client.send(request,
                java.net.http.HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Admin call failed: " + path + " -> " + response.statusCode());
        }
        return response.body();
    }

    public record MockStats(int requestCount, int processedCount, List<String> processedTransactionIds) {
    }
}
