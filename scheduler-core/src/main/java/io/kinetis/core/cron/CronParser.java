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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Parses a cron expression string into a {@link CronExpression}.
 * Auto-detects dialect from field count: 5 → UNIX, 6–7 → QUARTZ.
 *
 * <pre>
 *   *        all values       *\/5   every 5th value
 *   ?        any (day fields) 1-5    range
 *   1-5/2    stepped range    1,3,5  list
 *   L        last             15W    nearest weekday    2#3  Nth occurrence
 * </pre>
 */
public final class CronParser {

    private static final Map<String, Integer> MONTH_NAMES = Map.ofEntries(
            Map.entry("JAN", 1), Map.entry("FEB", 2),  Map.entry("MAR", 3),
            Map.entry("APR", 4), Map.entry("MAY", 5),  Map.entry("JUN", 6),
            Map.entry("JUL", 7), Map.entry("AUG", 8),  Map.entry("SEP", 9),
            Map.entry("OCT", 10), Map.entry("NOV", 11), Map.entry("DEC", 12));

    private static final Map<String, Integer> DOW_NAMES = Map.of(
            "MON", 1, "TUE", 2, "WED", 3, "THU", 4,
            "FRI", 5, "SAT", 6, "SUN", 7);

    private CronParser() {}

    /** Parse a cron expression, auto-detecting dialect from field count. */
    public static CronExpression parse(String expression) {
        if (expression == null || expression.isBlank())
            throw new CronParseException("cron expression must not be blank");
        String[] parts = expression.trim().split("\\s+");
        CronDialect dialect = switch (parts.length) {
            case 5    -> CronDialect.UNIX;
            case 6, 7 -> CronDialect.QUARTZ;
            default   -> throw new CronParseException(
                    "expected 5 (UNIX) or 6-7 (QUARTZ) fields, got " + parts.length + " in: " + expression);
        };
        return parse(expression, dialect);
    }

    public static CronExpression parse(String expression, CronDialect dialect) {
        if (expression == null || expression.isBlank())
            throw new CronParseException("cron expression must not be blank");
        String[] parts = expression.trim().split("\\s+");

        int offset = 0;
        FieldConstraint second = (dialect == CronDialect.QUARTZ)
                ? parseField(parts[offset++], 0, 59, FieldType.SECOND)
                : AllValues.INSTANCE;

        FieldConstraint minute     = parseField(parts[offset++], 0, 59, FieldType.MINUTE);
        FieldConstraint hour       = parseField(parts[offset++], 0, 23, FieldType.HOUR);
        FieldConstraint dayOfMonth = parseField(parts[offset++], 1, 31, FieldType.DAY_OF_MONTH);
        FieldConstraint month      = parseField(parts[offset++], 1, 12, FieldType.MONTH);
        FieldConstraint dayOfWeek  = parseField(parts[offset++], 1,  7, FieldType.DAY_OF_WEEK);

        Optional<FieldConstraint> year = Optional.empty();
        if (offset < parts.length)
            year = Optional.of(parseField(parts[offset], 1970, 2099, FieldType.YEAR));

        // Unix has no second field — fire at second :00 only
        if (dialect == CronDialect.UNIX) second = new Specific(0);

        return new CronExpression(second, minute, hour, dayOfMonth, month, dayOfWeek,
                year, expression, dialect);
    }

    private enum FieldType { SECOND, MINUTE, HOUR, DAY_OF_MONTH, MONTH, DAY_OF_WEEK, YEAR }

    private static FieldConstraint parseField(String token, int fieldMin, int fieldMax, FieldType type) {
        if (token.contains(",")) {
            String[] listParts = token.split(",");
            List<FieldConstraint> members = new ArrayList<>(listParts.length);
            for (String p : listParts) members.add(parseSingle(p.trim(), fieldMin, fieldMax, type));
            return new CompositeList(members);
        }
        return parseSingle(token, fieldMin, fieldMax, type);
    }

    private static FieldConstraint parseSingle(String token, int fieldMin, int fieldMax, FieldType type) {
        if ("*".equals(token)) return AllValues.INSTANCE;
        if ("?".equals(token)) {
            if (type != FieldType.DAY_OF_MONTH && type != FieldType.DAY_OF_WEEK)
                throw new CronParseException("'?' is only valid in day-of-month or day-of-week fields");
            return AnyValue.INSTANCE;
        }
        if ("L".equals(token)) return switch (type) {
            case DAY_OF_MONTH -> LastDayOfMonth.INSTANCE;
            case DAY_OF_WEEK  -> new LastDayOfWeek(7);
            default -> throw new CronParseException("'L' is only valid in day-of-month or day-of-week fields");
        };
        if (type == FieldType.DAY_OF_WEEK && token.endsWith("L") && token.length() > 1)
            return new LastDayOfWeek(resolveDow(token.substring(0, token.length() - 1)));
        if (type == FieldType.DAY_OF_MONTH && token.endsWith("W"))
            return new NearestWeekday(parseInt(token.substring(0, token.length() - 1), fieldMin, fieldMax, token));
        if (type == FieldType.DAY_OF_WEEK && token.contains("#")) {
            String[] hash = token.split("#");
            if (hash.length != 2) throw new CronParseException("invalid # expression: " + token);
            return new NthWeekday(resolveDow(hash[0]), parseInt(hash[1], 1, 5, token));
        }
        if (token.startsWith("*/"))
            return new Range(fieldMin, fieldMax, parseInt(token.substring(2), 1, fieldMax - fieldMin, token));
        if (token.contains("-")) {
            String[] rangeParts = token.split("/", 2);
            int step = rangeParts.length > 1 ? parseInt(rangeParts[1], 1, fieldMax - fieldMin, token) : 1;
            String[] bounds = rangeParts[0].split("-", 2);
            if (bounds.length != 2) throw new CronParseException("invalid range: " + token);
            return new Range(
                    resolveNameOrInt(bounds[0], type, fieldMin, fieldMax, token),
                    resolveNameOrInt(bounds[1], type, fieldMin, fieldMax, token),
                    step);
        }
        if (token.contains("/")) {
            String[] stepParts = token.split("/", 2);
            return new Range(
                    resolveNameOrInt(stepParts[0], type, fieldMin, fieldMax, token),
                    fieldMax,
                    parseInt(stepParts[1], 1, fieldMax - fieldMin, token));
        }
        return new Specific(resolveNameOrInt(token, type, fieldMin, fieldMax, token));
    }

    private static int resolveNameOrInt(String token, FieldType type, int min, int max, String context) {
        String upper = token.toUpperCase();
        if (type == FieldType.MONTH && MONTH_NAMES.containsKey(upper)) return MONTH_NAMES.get(upper);
        if (type == FieldType.DAY_OF_WEEK && DOW_NAMES.containsKey(upper)) return DOW_NAMES.get(upper);
        return parseInt(token, min, max, context);
    }

    private static int resolveDow(String token) {
        String upper = token.toUpperCase();
        if (DOW_NAMES.containsKey(upper)) return DOW_NAMES.get(upper);
        int v = parseInt(token, 0, 7, token);
        return v == 0 ? 7 : v; // 0=Sunday → ISO 7
    }

    private static int parseInt(String s, int min, int max, String context) {
        try {
            int v = Integer.parseInt(s.trim());
            if (v < min || v > max)
                throw new CronParseException("value " + v + " out of range [" + min + "-" + max + "] in: " + context);
            return v;
        } catch (NumberFormatException e) {
            throw new CronParseException("expected integer, got '" + s + "' in: " + context);
        }
    }
}
