package io.kinetis.core.cron.constraint;

/**
 * Matches values in {@code [min, max]} with an optional step.
 *
 * <ul>
 *   <li>{@code 1-5}   step=1 → matches 1,2,3,4,5</li>
 *   <li>{@code 1-5/2} step=2 → matches 1,3,5</li>
 *   <li>{@code *\/5}  min=fieldMin, max=fieldMax, step=5 → every 5th value</li>
 * </ul>
 */
public record Range(int min, int max, int step) implements FieldConstraint {

    public Range {
        if (min > max) throw new IllegalArgumentException("Range min (" + min + ") > max (" + max + ")");
        if (step < 1)  throw new IllegalArgumentException("Range step must be >= 1, got " + step);
    }

    public static Range of(int min, int max) {
        return new Range(min, max, 1);
    }

    @Override
    public boolean matches(int v) {
        if (v < min || v > max) return false;
        return (v - min) % step == 0;
    }

    /** Smallest value in this range that is {@code >= from}, or -1 if none. */
    public int nextFrom(int from) {
        if (from > max) return -1;
        int candidate = Math.max(from, min);
        int offset = (candidate - min) % step;
        if (offset != 0) candidate += (step - offset);
        return candidate > max ? -1 : candidate;
    }
}
