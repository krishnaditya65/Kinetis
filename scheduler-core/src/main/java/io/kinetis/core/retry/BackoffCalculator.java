package io.kinetis.core.retry;

import io.kinetis.core.model.RetryPolicy;

import java.time.Duration;
import java.util.random.RandomGenerator;

/**
 * Exponential backoff with full jitter, capped. Delay for 0-based attempt {@code n} is a random
 * value in {@code [0, min(cap, base * factor^n)]}.
 *
 * <p>Full jitter spreads retries of many jobs that failed together (e.g. a downstream outage)
 * across time instead of synchronising them into a thundering herd when the dependency recovers.
 */
public final class BackoffCalculator {

    /** Safety ceiling so factor^n can't overflow into absurd delays. */
    private static final long MAX_DELAY_MS = Duration.ofHours(1).toMillis();

    private final RandomGenerator random;

    public BackoffCalculator(RandomGenerator random) {
        this.random = random;
    }

    /**
     * @param attempt 0-based count of failures so far (0 = first retry delay)
     * @return delay before the next attempt
     */
    public Duration nextDelay(RetryPolicy policy, int attempt) {
        double raw = policy.backoffBaseMs() * Math.pow(policy.backoffFactor(), Math.max(0, attempt));
        long ceiling = (long) Math.min(raw, MAX_DELAY_MS);
        if (ceiling <= 0) return Duration.ZERO;
        return Duration.ofMillis(random.nextLong(ceiling + 1)); // full jitter in [0, ceiling]
    }
}
