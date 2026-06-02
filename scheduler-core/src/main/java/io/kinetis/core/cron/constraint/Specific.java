package io.kinetis.core.cron.constraint;

/** Matches exactly one integer value — e.g. {@code 5} in the minute field. */
public record Specific(int value) implements FieldConstraint {

    @Override
    public boolean matches(int v) { return v == value; }
}
