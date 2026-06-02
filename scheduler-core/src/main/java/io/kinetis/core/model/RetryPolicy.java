package io.kinetis.core.model;

/**
 * Per-job retry configuration. Delay for attempt {@code n} (0-based) is
 * {@code backoffBaseMs * backoffFactor^n}, plus jitter, capped at a maximum —
 * see {@link io.kinetis.core.retry.BackoffCalculator}.
 */
public record RetryPolicy(int maxAttempts, long backoffBaseMs, double backoffFactor) {

    public RetryPolicy {
        if (maxAttempts < 1) throw new IllegalArgumentException("maxAttempts must be >= 1");
        if (backoffBaseMs < 0) throw new IllegalArgumentException("backoffBaseMs must be >= 0");
        if (backoffFactor < 1.0) throw new IllegalArgumentException("backoffFactor must be >= 1.0");
    }

    public static RetryPolicy defaults() {
        return new RetryPolicy(3, 1_000L, 2.0);
    }
}
