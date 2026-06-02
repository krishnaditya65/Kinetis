package io.kinetis.core.model;

/**
 * What to do when a recurring job's fire time was missed (e.g. the scheduler was down).
 *
 * <ul>
 *   <li>{@link #SKIP} — forget the missed fire(s), schedule the next future occurrence.</li>
 *   <li>{@link #FIRE_ONCE} — run exactly one catch-up immediately, then resume normally.</li>
 *   <li>{@link #CATCH_UP} — run every missed occurrence (use with care: can stampede).</li>
 * </ul>
 */
public enum MisfirePolicy {
    SKIP,
    FIRE_ONCE,
    CATCH_UP
}
