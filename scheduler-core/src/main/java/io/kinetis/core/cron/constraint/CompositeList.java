package io.kinetis.core.cron.constraint;

import java.util.List;

/**
 * Matches if any member constraint matches — the {@code ,} operator.
 * E.g. {@code 1,15,20} is a CompositeList of three {@link Specific} constraints.
 */
public record CompositeList(List<FieldConstraint> parts) implements FieldConstraint {

    public CompositeList {
        if (parts == null || parts.isEmpty())
            throw new IllegalArgumentException("CompositeList must have at least one part");
        parts = List.copyOf(parts);
    }

    @Override
    public boolean matches(int value) {
        for (FieldConstraint part : parts) {
            if (part.matches(value)) return true;
        }
        return false;
    }
}
