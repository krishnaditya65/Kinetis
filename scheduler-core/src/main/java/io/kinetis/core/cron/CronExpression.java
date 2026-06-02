package io.kinetis.core.cron;

import io.kinetis.core.cron.constraint.FieldConstraint;

import java.util.Optional;

/**
 * A fully parsed cron expression. All fields are non-null; year is {@link Optional#empty()} when
 * not specified. The evaluator operates on this structure — never on raw strings.
 *
 * <p>Field naming follows Quartz conventions internally. Unix expressions are translated at parse
 * time: a {@code second} field of {@code AllValues} is inserted, year is empty.
 *
 * @param second      0–59
 * @param minute      0–59
 * @param hour        0–23
 * @param dayOfMonth  1–31 (also L, W variants)
 * @param month       1–12
 * @param dayOfWeek   1–7 ISO-8601 (1=Monday … 7=Sunday) (also L, # variants)
 * @param year        1970–2099, or empty
 * @param original    original expression string, for logging/error messages
 * @param dialect     the dialect this was parsed in
 */
public record CronExpression(
        FieldConstraint second,
        FieldConstraint minute,
        FieldConstraint hour,
        FieldConstraint dayOfMonth,
        FieldConstraint month,
        FieldConstraint dayOfWeek,
        Optional<FieldConstraint> year,
        String original,
        CronDialect dialect
) {
}
