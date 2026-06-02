package io.kinetis.core.cron.constraint;

/**
 * The {@code ?} specifier — used in day-of-month or day-of-week to mean "don't care about
 * this field; use the other one." Semantically identical to {@link AllValues} at evaluation time;
 * the distinction matters only when reconciling the two day fields against each other.
 */
public record AnyValue() implements FieldConstraint {

    public static final AnyValue INSTANCE = new AnyValue();

    @Override
    public boolean matches(int value) { return true; }
}
