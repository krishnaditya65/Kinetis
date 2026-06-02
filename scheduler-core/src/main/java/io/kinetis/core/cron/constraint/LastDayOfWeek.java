package io.kinetis.core.cron.constraint;

/**
 * The {@code nL} specifier in day-of-week — matches the last occurrence of weekday {@code n}
 * (1=Monday … 7=Sunday) in the month. E.g. {@code 6L} = last Saturday of the month.
 * Requires full date context; evaluated by {@code CronEvaluator}.
 */
public record LastDayOfWeek(int dayOfWeek) implements FieldConstraint {

    public LastDayOfWeek {
        if (dayOfWeek < 1 || dayOfWeek > 7)
            throw new IllegalArgumentException("dayOfWeek must be 1-7, got " + dayOfWeek);
    }
}
