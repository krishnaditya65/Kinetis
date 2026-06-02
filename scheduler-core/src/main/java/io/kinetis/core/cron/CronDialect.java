package io.kinetis.core.cron;

/**
 * Which cron format to parse.
 *
 * <ul>
 *   <li>{@link #UNIX} — 5 fields: {@code minute hour day-of-month month day-of-week}</li>
 *   <li>{@link #QUARTZ} — 6 fields: {@code second minute hour day-of-month month day-of-week},
 *       optional 7th year field. Supports {@code L}, {@code W}, {@code #} specifiers.</li>
 * </ul>
 */
public enum CronDialect {
    UNIX,
    QUARTZ;

    public int mandatoryFieldCount() {
        return this == UNIX ? 5 : 6;
    }
}
