package io.kinetis.core.cron;

import io.kinetis.core.cron.constraint.AllValues;
import io.kinetis.core.cron.constraint.AnyValue;
import io.kinetis.core.cron.constraint.CompositeList;
import io.kinetis.core.cron.constraint.FieldConstraint;
import io.kinetis.core.cron.constraint.LastDayOfMonth;
import io.kinetis.core.cron.constraint.LastDayOfWeek;
import io.kinetis.core.cron.constraint.NearestWeekday;
import io.kinetis.core.cron.constraint.NthWeekday;
import io.kinetis.core.cron.constraint.Range;
import io.kinetis.core.cron.constraint.Specific;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Computes the next fire time for a {@link CronExpression} relative to a given instant.
 *
 * <h2>Algorithm</h2>
 * Starting from {@code from + 1 second}, advance field by field from largest (year) to smallest
 * (second). When a field doesn't match, advance to the next matching value and reset all smaller
 * fields to their minimum. Repeat until all fields match, or the candidate exceeds 4 years.
 *
 * <h2>DST correctness</h2>
 * All arithmetic uses {@link ZonedDateTime} so Java handles DST transitions:
 * spring-forward gaps are skipped naturally; fall-back overlaps fire at both wall-clock occurrences.
 */
public final class CronEvaluator {

    private static final int MAX_ITERATIONS = 4 * 366 * 24 * 60; // ~4 years in minutes

    private CronEvaluator() {}

    /**
     * Compute the next fire time strictly after {@code from}.
     *
     * @throws CronParseException if no fire time is found within 4 years
     */
    public static ZonedDateTime next(CronExpression expr, ZonedDateTime from) {
        ZonedDateTime candidate = from.truncatedTo(ChronoUnit.SECONDS).plusSeconds(1);
        int iterations = 0;

        while (iterations++ < MAX_ITERATIONS) {

            // Year
            if (expr.year().isPresent()) {
                FieldConstraint yr = expr.year().get();
                int year = candidate.getYear();
                if (!yr.matches(year)) {
                    int next = nextMatchingInt(yr, year + 1, 1970, 2099);
                    if (next == -1) break;
                    candidate = candidate.withYear(next).withMonth(1).withDayOfMonth(1)
                            .truncatedTo(ChronoUnit.DAYS);
                    continue;
                }
            }

            // Month
            int month = candidate.getMonthValue();
            if (!expr.month().matches(month)) {
                int next = nextMatchingInt(expr.month(), month + 1, 1, 12);
                candidate = (next == -1)
                        ? candidate.plusYears(1).withMonth(1).withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS)
                        : candidate.withMonth(next).withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS);
                continue;
            }

            // Day
            if (!dayMatches(expr, candidate)) {
                candidate = candidate.plusDays(1).truncatedTo(ChronoUnit.DAYS);
                continue;
            }

            // Hour
            int hour = candidate.getHour();
            if (!expr.hour().matches(hour)) {
                int next = nextMatchingInt(expr.hour(), hour + 1, 0, 23);
                candidate = (next == -1)
                        ? candidate.plusDays(1).truncatedTo(ChronoUnit.DAYS)
                        : candidate.withHour(next).truncatedTo(ChronoUnit.HOURS);
                continue;
            }

            // Minute
            int minute = candidate.getMinute();
            if (!expr.minute().matches(minute)) {
                int next = nextMatchingInt(expr.minute(), minute + 1, 0, 59);
                candidate = (next == -1)
                        ? candidate.plusHours(1).truncatedTo(ChronoUnit.HOURS)
                        : candidate.withMinute(next).withSecond(0).withNano(0);
                continue;
            }

            // Second
            int second = candidate.getSecond();
            if (!expr.second().matches(second)) {
                int next = nextMatchingInt(expr.second(), second + 1, 0, 59);
                candidate = (next == -1)
                        ? candidate.plusMinutes(1).withSecond(0).withNano(0)
                        : candidate.withSecond(next).withNano(0);
                continue;
            }

            return candidate;
        }

        throw new CronParseException("No next fire time found within 4 years for: " + expr.original());
    }

    /** All fire times strictly after {@code from} and before {@code until}. */
    public static List<ZonedDateTime> allBetween(CronExpression expr,
                                                  ZonedDateTime from, ZonedDateTime until) {
        List<ZonedDateTime> result = new ArrayList<>();
        ZonedDateTime cursor = from;
        while (true) {
            ZonedDateTime fire;
            try {
                fire = next(expr, cursor);
            } catch (CronParseException e) {
                break;
            }
            if (!fire.isBefore(until)) break;
            result.add(fire);
            cursor = fire;
        }
        return result;
    }

    // ---- Day matching -------------------------------------------------------

    /**
     * Day-of-month and day-of-week interact:
     * - If either is {@code ?}, only the other is checked.
     * - If both are {@code *}, any day matches.
     * - If both are specific, EITHER matching suffices (OR semantics).
     */
    static boolean dayMatches(CronExpression expr, ZonedDateTime candidate) {
        boolean domIsAny = isAny(expr.dayOfMonth());
        boolean dowIsAny = isAny(expr.dayOfWeek());
        if (domIsAny && dowIsAny) return true;
        if (domIsAny) return dowMatches(expr.dayOfWeek(), candidate);
        if (dowIsAny) return domMatches(expr.dayOfMonth(), candidate);
        return domMatches(expr.dayOfMonth(), candidate) || dowMatches(expr.dayOfWeek(), candidate);
    }

    private static boolean isAny(FieldConstraint c) {
        return c instanceof AllValues || c instanceof AnyValue;
    }

    private static boolean domMatches(FieldConstraint dom, ZonedDateTime dt) {
        int day = dt.getDayOfMonth();
        return switch (dom) {
            case LastDayOfMonth ignored -> day == dt.toLocalDate().lengthOfMonth();
            case NearestWeekday nw      -> day == nearestWeekday(nw.day(), dt.toLocalDate());
            case CompositeList cl       -> cl.parts().stream().anyMatch(p -> domMatches(p, dt));
            default                     -> dom.matches(day);
        };
    }

    private static boolean dowMatches(FieldConstraint dow, ZonedDateTime dt) {
        int isoDow = dt.getDayOfWeek().getValue();
        return switch (dow) {
            case LastDayOfWeek ldow -> isoDow == ldow.dayOfWeek()
                    && isLastOccurrenceInMonth(dt.toLocalDate());
            case NthWeekday nth -> isoDow == nth.dayOfWeek()
                    && nthOccurrenceInMonth(dt.toLocalDate(), nth.dayOfWeek()) == nth.n();
            case CompositeList cl -> cl.parts().stream().anyMatch(p -> dowMatches(p, dt));
            default               -> dow.matches(isoDow);
        };
    }

    // ---- Calendar helpers ---------------------------------------------------

    private static int nearestWeekday(int targetDay, LocalDate ref) {
        int lastDay = ref.lengthOfMonth();
        int day = Math.min(targetDay, lastDay);
        LocalDate d = ref.withDayOfMonth(day);
        DayOfWeek dow = d.getDayOfWeek();
        if (dow == DayOfWeek.SATURDAY) return day == 1 ? day + 2 : day - 1;
        if (dow == DayOfWeek.SUNDAY)   return day == lastDay ? day - 2 : day + 1;
        return day;
    }

    private static boolean isLastOccurrenceInMonth(LocalDate date) {
        return date.plusWeeks(1).getMonth() != date.getMonth();
    }

    private static int nthOccurrenceInMonth(LocalDate date, int isoDow) {
        int count = 0;
        LocalDate cursor = date.withDayOfMonth(1);
        while (!cursor.isAfter(date)) {
            if (cursor.getDayOfWeek().getValue() == isoDow) count++;
            cursor = cursor.plusDays(1);
        }
        return count;
    }

    // ---- Field-advancement helper -------------------------------------------

    static int nextMatchingInt(FieldConstraint constraint, int from, int fieldMin, int fieldMax) {
        return switch (constraint) {
            case AllValues ignored  -> from <= fieldMax ? from : -1;
            case AnyValue ignored   -> from <= fieldMax ? from : -1;
            case Specific s         -> s.value() >= from && s.value() <= fieldMax ? s.value() : -1;
            case Range r            -> r.nextFrom(from);
            case CompositeList cl   -> {
                int best = -1;
                for (FieldConstraint part : cl.parts()) {
                    int c = nextMatchingInt(part, from, fieldMin, fieldMax);
                    if (c != -1 && (best == -1 || c < best)) best = c;
                }
                yield best;
            }
            default -> from <= fieldMax ? from : -1;
        };
    }
}
