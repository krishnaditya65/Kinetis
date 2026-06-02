package io.kinetis.core.cron.constraint;

/**
 * The {@code dow#n} specifier — the Nth occurrence of a weekday within the month.
 * {@code dayOfWeek} uses ISO-8601 (1=Monday … 7=Sunday). E.g. {@code 2#3} = 3rd Tuesday.
 * Requires full date context; evaluated by {@code CronEvaluator}.
 */
public record NthWeekday(int dayOfWeek, int n) implements FieldConstraint {

    public NthWeekday {
        if (dayOfWeek < 1 || dayOfWeek > 7)
            throw new IllegalArgumentException("dayOfWeek must be 1-7, got " + dayOfWeek);
        if (n < 1 || n > 5)
            throw new IllegalArgumentException("n must be 1-5, got " + n);
    }
}
