package io.kinetis.core.webhook;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Fires HTTP POST webhooks when a job or workflow reaches a terminal state.
 *
 * <h2>Delivery guarantees</h2>
 * At-least-once: if the initial POST fails (5xx, timeout, network error) the dispatcher
 * retries up to {@code maxAttempts} times with exponential backoff. Retries are handled
 * by a virtual-thread pool — they don't block the caller.
 *
 * <h2>Payload format</h2>
 * <pre>
 * {
 *   "event":      "job.succeeded" | "job.failed" | "workflow.succeeded" | "workflow.failed",
 *   "resourceId": "&lt;UUID&gt;",
 *   "state":      "SUCCEEDED" | "DEAD_LETTER" | ...,
 *   "at":         "&lt;ISO-8601 instant&gt;"
 * }
 * </pre>
 */
public class WebhookDispatcher {

    private static final Logger log = LoggerFactory.getLogger(WebhookDispatcher.class);

    private static final int     MAX_ATTEMPTS      = 3;
    private static final long    INITIAL_BACKOFF_MS = 500;
    private static final Duration TIMEOUT           = Duration.ofSeconds(10);

    private final HttpClient httpClient;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public WebhookDispatcher() {
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(TIMEOUT)
                .executor(executor)
                .build();
    }

    /** Fire-and-forget: POST the event asynchronously, retry on failure. */
    public void fire(String callbackUrl, String event, String resourceId, String state) {
        if (callbackUrl == null || callbackUrl.isBlank()) return;
        executor.submit(() -> deliver(callbackUrl, buildPayload(event, resourceId, state)));
    }

    private void deliver(String url, String payload) {
        long backoffMs = INITIAL_BACKOFF_MS;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("Content-Type", "application/json")
                        .header("User-Agent", "Kinetis-Webhook/1.0")
                        .POST(HttpRequest.BodyPublishers.ofString(payload))
                        .timeout(TIMEOUT)
                        .build();
                HttpResponse<Void> response = httpClient.send(request,
                        HttpResponse.BodyHandlers.discarding());
                int status = response.statusCode();
                if (status >= 200 && status < 300) {
                    log.debug("webhook delivered to {} (status={})", url, status);
                    return;
                }
                log.warn("webhook {} returned {} (attempt {}/{})", url, status, attempt, MAX_ATTEMPTS);
            } catch (Exception e) {
                log.warn("webhook delivery failed to {} (attempt {}/{}): {}", url, attempt, MAX_ATTEMPTS, e.getMessage());
            }
            if (attempt < MAX_ATTEMPTS) {
                try { Thread.sleep(backoffMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
                backoffMs *= 2;
            }
        }
        log.error("webhook permanently failed after {} attempts: {}", MAX_ATTEMPTS, url);
    }

    private static String buildPayload(String event, String resourceId, String state) {
        return "{\"event\":\"" + event + "\","
                + "\"resourceId\":\"" + resourceId + "\","
                + "\"state\":\"" + state + "\","
                + "\"at\":\"" + java.time.Instant.now() + "\"}";
    }
}
