package io.kinetis.core.cron.constraint;

/**
 * The {@code L} specifier in day-of-month — matches the last calendar day of the month
 * (28–31 depending on month/year). Requires full date context; evaluated by {@code CronEvaluator}.
 */
public record LastDayOfMonth() implements FieldConstraint {

    public static final LastDayOfMonth INSTANCE = new LastDayOfMonth();
}
