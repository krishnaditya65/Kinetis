package io.kinetis.core.queue;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

/**
 * Token-bucket rate limiter per tenant, backed by the {@code rate_limits} Postgres table.
 *
 * <ul>
 *   <li><b>Consume</b> — atomically deducts one token if available ({@code WHERE tokens >= 1}).
 *       Returns {@code false} (rate-limited) only if the bucket is empty AND the tenant has a
 *       configured limit; tenants with no entry are unlimited.</li>
 *   <li><b>Refill</b> — adds {@code refill_rate × elapsed_seconds} tokens up to {@code max_tokens}.
 *       Uses wall-clock elapsed time so missed refill ticks self-correct automatically.</li>
 *   <li><b>Upsert</b> — create or update a tenant's limit config; resets tokens to the new max.</li>
 * </ul>
 */
public class RateLimiter {

    private final JdbcTemplate jdbc;

    public RateLimiter(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * Try to consume one token for {@code tenantId}.
     *
     * @return {@code true} if a token was consumed or the tenant is unlimited;
     *         {@code false} if the bucket is empty (rate-limited)
     */
    public boolean tryConsume(String tenantId) {
        if (tenantId == null || tenantId.isBlank()) return true;
        int updated = jdbc.update("""
                UPDATE rate_limits SET tokens = tokens - 1
                WHERE tenant_id = ? AND tokens >= 1
                """, tenantId);
        if (updated > 0) return true;
        // 0 rows: bucket empty OR no limit configured — distinguish by existence check
        Integer count = jdbc.queryForObject(
                "SELECT count(*) FROM rate_limits WHERE tenant_id = ?", Integer.class, tenantId);
        return count == null || count == 0; // not in table = unlimited
    }

    /**
     * Refill all tenant buckets. Call once per second from a background task.
     * Self-correcting: elapsed time since {@code last_refill} is used, not a fixed interval.
     */
    public int refillAll() {
        return jdbc.update("""
                UPDATE rate_limits
                SET tokens      = LEAST(max_tokens, tokens + refill_rate *
                                    EXTRACT(EPOCH FROM (now() - last_refill))),
                    last_refill = now()
                WHERE last_refill < now() - interval '100 milliseconds'
                """);
    }

    /**
     * Create or update a tenant's rate limit. Tokens are reset to {@code maxTokens} on change.
     *
     * @param maxTokens  burst capacity
     * @param refillRate tokens added per second
     * @param weight     relative fair-share weight (1 = normal)
     */
    public void setLimit(String tenantId, double maxTokens, double refillRate, int weight) {
        jdbc.update("""
                INSERT INTO rate_limits (tenant_id, tokens, max_tokens, refill_rate, weight)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (tenant_id) DO UPDATE
                  SET max_tokens  = EXCLUDED.max_tokens,
                      refill_rate = EXCLUDED.refill_rate,
                      weight      = EXCLUDED.weight,
                      tokens      = EXCLUDED.max_tokens
                """, tenantId, maxTokens, maxTokens, refillRate, weight);
    }

    /** Remove a tenant's rate limit (becomes unlimited). */
    public void removeLimit(String tenantId) {
        jdbc.update("DELETE FROM rate_limits WHERE tenant_id = ?", tenantId);
    }

    /** Current token count for a tenant, or -1.0 if no limit configured (unlimited). */
    public double currentTokens(String tenantId) {
        List<Double> rows = jdbc.query(
                "SELECT tokens FROM rate_limits WHERE tenant_id = ?",
                (rs, r) -> rs.getDouble(1), tenantId);
        return rows.isEmpty() ? -1.0 : rows.get(0);
    }
}
