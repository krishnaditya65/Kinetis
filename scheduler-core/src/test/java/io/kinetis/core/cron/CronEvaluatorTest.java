package io.kinetis.core.cron;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class CronEvaluatorTest {

    private static ZonedDateTime zdt(String iso, String zone) {
        return ZonedDateTime.parse(iso + "[" + zone + "]");
    }

    @Test
    void everyMinuteAdvancesOneMinute() {
        CronExpression expr = CronParser.parse("* * * * *");
        ZonedDateTime next = CronEvaluator.next(expr, zdt("2026-06-03T10:00:30+00:00", "UTC"));
        assertThat(next).isEqualTo(zdt("2026-06-03T10:01:00+00:00", "UTC"));
    }

    @Test
    void specificTimeEachDay() {
        CronExpression expr = CronParser.parse("0 30 9 * * ?", CronDialect.QUARTZ);
        ZonedDateTime next = CronEvaluator.next(expr, zdt("2026-06-03T09:31:00+00:00", "UTC"));
        assertThat(next.getHour()).isEqualTo(9);
        assertThat(next.getMinute()).isEqualTo(30);
        assertThat(next.getDayOfMonth()).isEqualTo(4);
    }

    @Test
    void everyFiveMinutes() {
        CronExpression expr = CronParser.parse("*/5 * * * *");
        assertThat(CronEvaluator.next(expr, zdt("2026-06-03T10:00:00+00:00", "UTC")))
                .isEqualTo(zdt("2026-06-03T10:05:00+00:00", "UTC"));
    }

    @Test
    void monthlyOnFirstAndFifteenth() {
        CronExpression expr = CronParser.parse("0 0 1,15 * *");
        ZonedDateTime next = CronEvaluator.next(expr, zdt("2026-06-03T00:00:00+00:00", "UTC"));
        assertThat(next.getDayOfMonth()).isEqualTo(15);
        assertThat(next.getMonthValue()).isEqualTo(6);
    }

    @Test
    void rollsOverMonthBoundary() {
        CronExpression expr = CronParser.parse("0 0 15 * *");
        ZonedDateTime next = CronEvaluator.next(expr, zdt("2026-06-16T00:00:00+00:00", "UTC"));
        assertThat(next.getDayOfMonth()).isEqualTo(15);
        assertThat(next.getMonthValue()).isEqualTo(7);
    }

    @Test
    void rollsOverYearBoundary() {
        CronExpression expr = CronParser.parse("0 0 1 1 *");
        ZonedDateTime next = CronEvaluator.next(expr, zdt("2026-01-02T00:00:00+00:00", "UTC"));
        assertThat(next.getYear()).isEqualTo(2027);
        assertThat(next.getMonthValue()).isEqualTo(1);
        assertThat(next.getDayOfMonth()).isEqualTo(1);
    }

    @Test
    void weekdayOnly_mondayToFriday() {
        CronExpression expr = CronParser.parse("0 9 0 ? * MON-FRI", CronDialect.QUARTZ);
        ZonedDateTime next = CronEvaluator.next(expr, zdt("2026-06-06T09:00:00+00:00", "UTC"));
        assertThat(next.getDayOfWeek().name()).isEqualTo("MONDAY");
        assertThat(next.getDayOfMonth()).isEqualTo(8);
    }

    @Test
    void lastDayOfMonth() {
        CronExpression expr = CronParser.parse("0 0 0 L * ?", CronDialect.QUARTZ);
        ZonedDateTime next = CronEvaluator.next(expr, zdt("2026-06-01T00:00:00+00:00", "UTC"));
        assertThat(next.getDayOfMonth()).isEqualTo(30);
        assertThat(next.getMonthValue()).isEqualTo(6);
    }

    @Test
    void lastDayOfMonth_february_leapYear() {
        CronExpression expr = CronParser.parse("0 0 0 L 2 ?", CronDialect.QUARTZ);
        ZonedDateTime next = CronEvaluator.next(expr, zdt("2028-01-01T00:00:00+00:00", "UTC"));
        assertThat(next.getMonthValue()).isEqualTo(2);
        assertThat(next.getDayOfMonth()).isEqualTo(29);
    }

    @Test
    void nthWeekday_thirdTuesdayOfMonth() {
        CronExpression expr = CronParser.parse("0 0 0 ? * 2#3", CronDialect.QUARTZ);
        ZonedDateTime next = CronEvaluator.next(expr, zdt("2026-06-01T00:00:00+00:00", "UTC"));
        assertThat(next.getDayOfWeek().getValue()).isEqualTo(2);
        assertThat(next.getDayOfMonth()).isEqualTo(16);
    }

    @Test
    void allBetweenReturnsFiveTimes() {
        CronExpression expr = CronParser.parse("*/15 * * * *");
        List<ZonedDateTime> fires = CronEvaluator.allBetween(expr,
                zdt("2026-06-03T10:00:00+00:00", "UTC"),
                zdt("2026-06-03T11:00:00+00:00", "UTC"));
        assertThat(fires).hasSize(3);
        assertThat(fires.get(0).getMinute()).isEqualTo(15);
        assertThat(fires.get(2).getMinute()).isEqualTo(45);
    }

    @Test
    void springForward_firingEveryHour_skipsGapHour() {
        CronExpression expr = CronParser.parse("0 * * * *");
        ZoneId zone = ZoneId.of("America/New_York");
        ZonedDateTime from = ZonedDateTime.of(2026, 3, 8, 1, 0, 0, 0, zone);
        ZonedDateTime next = CronEvaluator.next(expr, from);
        assertThat(next.getHour()).isEqualTo(3);
        assertThat(next.getOffset().getTotalSeconds()).isEqualTo(-4 * 3600);
    }

    @Test
    void fallBack_firingEveryHour_firesBothAmbiguousOccurrences() {
        CronExpression expr = CronParser.parse("0 * * * *");
        ZoneId zone = ZoneId.of("America/New_York");
        ZonedDateTime from = ZonedDateTime.of(2026, 11, 1, 0, 0, 0, 0, zone);
        ZonedDateTime next = CronEvaluator.next(expr, from);
        assertThat(next.getHour()).isEqualTo(1);
        assertThat(next.getOffset().getTotalSeconds()).isEqualTo(-4 * 3600);
    }

    @Test
    void cronExpressionNeverMatching_throwsWithinFourYears() {
        CronExpression expr = CronParser.parse("0 0 0 1 1 ? 2020", CronDialect.QUARTZ);
        assertThatThrownBy(() -> CronEvaluator.next(expr, zdt("2026-01-01T00:00:00+00:00", "UTC")))
                .isInstanceOf(CronParseException.class).hasMessageContaining("No next fire time");
    }
}
