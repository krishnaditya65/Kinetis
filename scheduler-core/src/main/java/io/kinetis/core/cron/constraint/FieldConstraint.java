package io.kinetis.core.cron.constraint;

/**
 * A constraint on one cron field (second, minute, hour, etc.).
 *
 * <pre>
 *   *       AllValues          — every value
 *   ?       AnyValue           — don't care (day fields only)
 *   5       Specific           — exact value
 *   1-5     Range              — inclusive range, optional step
 *   1,3,5   CompositeList      — union of constraints
 *   L       LastDayOfMonth     — last day of month
 *   15W     NearestWeekday     — nearest weekday to day N
 *   5L      LastDayOfWeek      — last weekday N in month
 *   2#3     NthWeekday         — Nth occurrence of weekday in month
 * </pre>
 *
 * Day-level constraints (L, W, #) require full date context and are evaluated by
 * {@code CronEvaluator.dayMatches()}; they throw on the plain {@link #matches(int)} form.
 */
public sealed interface FieldConstraint
        permits AllValues, AnyValue, Specific, Range, CompositeList,
                LastDayOfMonth, NearestWeekday, LastDayOfWeek, NthWeekday {

    default boolean matches(int value) {
        throw new UnsupportedOperationException(
                "This constraint requires date context — use CronEvaluator.dayMatches()");
    }
}
