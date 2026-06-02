package io.kinetis.core.cron;

import io.kinetis.core.cron.constraint.*;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class CronParserTest {

    @Test
    void parsesUnixFiveFieldExpression() {
        CronExpression expr = CronParser.parse("0 12 * * 1");
        assertThat(expr.dialect()).isEqualTo(CronDialect.UNIX);
        assertThat(expr.second()).isInstanceOf(Specific.class);
        assertThat(((Specific) expr.second()).value()).isZero();
        assertThat(((Specific) expr.hour()).value()).isEqualTo(12);
        assertThat(expr.dayOfMonth()).isInstanceOf(AllValues.class);
        assertThat(((Specific) expr.dayOfWeek()).value()).isEqualTo(1);
    }

    @Test
    void parsesQuartzSixFieldExpression() {
        CronExpression expr = CronParser.parse("0 30 9 * * MON-FRI");
        assertThat(expr.dialect()).isEqualTo(CronDialect.QUARTZ);
        assertThat(((Specific) expr.minute()).value()).isEqualTo(30);
        assertThat(((Specific) expr.hour()).value()).isEqualTo(9);
        Range dow = (Range) expr.dayOfWeek();
        assertThat(dow.min()).isEqualTo(1);
        assertThat(dow.max()).isEqualTo(5);
    }

    @Test
    void parsesEveryFiveMinutes() {
        Range r = (Range) CronParser.parse("*/5 * * * *").minute();
        assertThat(r.min()).isZero();
        assertThat(r.max()).isEqualTo(59);
        assertThat(r.step()).isEqualTo(5);
    }

    @Test
    void parsesCommaList() {
        CompositeList list = (CompositeList) CronParser.parse("0 0 1,15 * *").dayOfMonth();
        assertThat(list.parts()).hasSize(2);
        assertThat(((Specific) list.parts().get(0)).value()).isEqualTo(1);
        assertThat(((Specific) list.parts().get(1)).value()).isEqualTo(15);
    }

    @Test
    void parsesLastDayOfMonth() {
        CronExpression expr = CronParser.parse("0 0 0 L * ?", CronDialect.QUARTZ);
        assertThat(expr.dayOfMonth()).isInstanceOf(LastDayOfMonth.class);
        assertThat(expr.dayOfWeek()).isInstanceOf(AnyValue.class);
    }

    @Test
    void parsesNearestWeekday() {
        NearestWeekday nw = (NearestWeekday) CronParser.parse("0 0 0 15W * ?", CronDialect.QUARTZ).dayOfMonth();
        assertThat(nw.day()).isEqualTo(15);
    }

    @Test
    void parsesNthWeekday() {
        NthWeekday nth = (NthWeekday) CronParser.parse("0 0 0 ? * 2#3", CronDialect.QUARTZ).dayOfWeek();
        assertThat(nth.dayOfWeek()).isEqualTo(2);
        assertThat(nth.n()).isEqualTo(3);
    }

    @Test
    void parsesLastDayOfWeek() {
        LastDayOfWeek ldw = (LastDayOfWeek) CronParser.parse("0 0 0 ? * 5L", CronDialect.QUARTZ).dayOfWeek();
        assertThat(ldw.dayOfWeek()).isEqualTo(5);
    }

    @Test
    void parsesMonthNamesAndSteps() {
        Range r = (Range) CronParser.parse("0 0 0 1 JAN-JUN/2 ?", CronDialect.QUARTZ).month();
        assertThat(r.min()).isEqualTo(1);
        assertThat(r.max()).isEqualTo(6);
        assertThat(r.step()).isEqualTo(2);
    }

    @Test
    void rejectsWrongFieldCount() {
        assertThatThrownBy(() -> CronParser.parse("* * *"))
                .isInstanceOf(CronParseException.class).hasMessageContaining("expected 5");
    }

    @Test
    void rejectsOutOfRangeValue() {
        assertThatThrownBy(() -> CronParser.parse("0 60 * * *"))
                .isInstanceOf(CronParseException.class).hasMessageContaining("out of range");
    }

    @Test
    void rejectsQuestionMarkInNonDayField() {
        assertThatThrownBy(() -> CronParser.parse("0 ? * * *"))
                .isInstanceOf(CronParseException.class).hasMessageContaining("only valid in day");
    }
}
