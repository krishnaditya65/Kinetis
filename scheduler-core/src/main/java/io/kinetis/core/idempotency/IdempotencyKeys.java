package io.kinetis.core.idempotency;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;

/**
 * Derives idempotency keys at two distinct levels:
 *
 * <ul>
 *   <li><b>Job key</b> — deduplicates submission. Re-submitting the same logical job is a no-op
 *       (enforced by a UNIQUE constraint on {@code jobs.idempotency_key}). Derived from
 *       type + payload when the caller doesn't supply one.</li>
 *   <li><b>Run key</b> — deduplicates a single execution's effect. Retries of the same run share
 *       the key (intended — effects should be idempotent). Each new cron occurrence gets a
 *       different key because it includes the schedule slot, so this hour's run never falsely
 *       deduplicates against last hour's.</li>
 * </ul>
 */
public final class IdempotencyKeys {

    private IdempotencyKeys() {}

    /** Job-level key when the caller didn't supply one: stable hash of type + payload. */
    public static String deriveJobKey(String jobType, String payload) {
        return "job:" + sha256(jobType + " " + (payload == null ? "" : payload));
    }

    /**
     * Run-level key = job key + schedule slot. The slot makes each cron occurrence distinct
     * while keeping retries of the same occurrence deduplicated.
     */
    public static String deriveRunKey(String jobKey, Instant scheduledFor) {
        return jobKey + ":" + scheduledFor.toEpochMilli();
    }

    private static String sha256(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
