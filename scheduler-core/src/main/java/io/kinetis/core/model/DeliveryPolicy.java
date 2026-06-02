package io.kinetis.core.model;

/**
 * Delivery semantics for a job.
 *
 * <ul>
 *   <li>{@link #AT_LEAST_ONCE} — the default. The system retries until a worker positively
 *       acknowledges success; combined with idempotency keys this yields effectively-once effects.</li>
 *   <li>{@link #AT_MOST_ONCE} — opt-out for jobs where a duplicate is worse than a miss
 *       (e.g. one-time OTP). Never retried; a crash means the run is lost by design.</li>
 * </ul>
 */
public enum DeliveryPolicy {
    AT_LEAST_ONCE,
    AT_MOST_ONCE
}
