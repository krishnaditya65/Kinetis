package io.kinetis.core.queue;

import io.kinetis.core.AbstractPgTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RateLimiterTest extends AbstractPgTest {

    @Test
    void tenantWithNoLimitIsAlwaysAllowed() {
        RateLimiter limiter = new RateLimiter(jdbc);
        assertThat(limiter.tryConsume("unknown-tenant")).isTrue();
        assertThat(limiter.tryConsume(null)).isTrue();
        assertThat(limiter.tryConsume("")).isTrue();
    }

    @Test
    void tenantWithFullBucketIsAllowed() {
        RateLimiter limiter = new RateLimiter(jdbc);
        limiter.setLimit("tenant-a", 10.0, 1.0, 1);
        assertThat(limiter.tryConsume("tenant-a")).isTrue();
        assertThat(limiter.currentTokens("tenant-a")).isLessThan(10.0);
    }

    @Test
    void tenantExhaustsBucketAndIsRateLimited() {
        RateLimiter limiter = new RateLimiter(jdbc);
        limiter.setLimit("tenant-b", 3.0, 0.0, 1);
        assertThat(limiter.tryConsume("tenant-b")).isTrue();
        assertThat(limiter.tryConsume("tenant-b")).isTrue();
        assertThat(limiter.tryConsume("tenant-b")).isTrue();
        assertThat(limiter.tryConsume("tenant-b")).isFalse();
    }

    @Test
    void refillRestoresTokens() {
        RateLimiter limiter = new RateLimiter(jdbc);
        limiter.setLimit("tenant-c", 5.0, 5.0, 1);
        for (int i = 0; i < 5; i++) limiter.tryConsume("tenant-c");
        assertThat(limiter.tryConsume("tenant-c")).isFalse();

        jdbc.update("UPDATE rate_limits SET last_refill = now() - interval '2 seconds' WHERE tenant_id = 'tenant-c'");
        limiter.refillAll();
        assertThat(limiter.tryConsume("tenant-c")).isTrue();
    }

    @Test
    void removingLimitMakesTenantUnlimited() {
        RateLimiter limiter = new RateLimiter(jdbc);
        limiter.setLimit("tenant-d", 1.0, 0.0, 1);
        assertThat(limiter.tryConsume("tenant-d")).isTrue();
        assertThat(limiter.tryConsume("tenant-d")).isFalse();
        limiter.removeLimit("tenant-d");
        assertThat(limiter.tryConsume("tenant-d")).isTrue();
    }

    @Test
    void setLimitUpdatesExistingEntry() {
        RateLimiter limiter = new RateLimiter(jdbc);
        limiter.setLimit("tenant-e", 5.0, 1.0, 1);
        limiter.setLimit("tenant-e", 100.0, 50.0, 2);
        assertThat(limiter.currentTokens("tenant-e")).isEqualTo(100.0);
    }
}
