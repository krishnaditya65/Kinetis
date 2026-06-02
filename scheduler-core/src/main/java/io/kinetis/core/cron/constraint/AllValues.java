package io.kinetis.core.cron.constraint;

/** Matches every value in the field's range — the {@code *} specifier. */
public record AllValues() implements FieldConstraint {

    public static final AllValues INSTANCE = new AllValues();

    @Override
    public boolean matches(int value) { return true; }
}
