package io.kinetis.core.cron.constraint;

/**
 * The {@code nW} specifier — nearest weekday (Mon–Fri) to day-of-month {@code n}.
 * Saturday → preceding Friday. Sunday → following Monday. Never crosses a month boundary.
 * Requires full date context; evaluated by {@code CronEvaluator}.
 */
public record NearestWeekday(int day) implements FieldConstraint {

    public NearestWeekday {
        if (day < 1 || day > 31)
            throw new IllegalArgumentException("day must be 1-31, got " + day);
    }
}
