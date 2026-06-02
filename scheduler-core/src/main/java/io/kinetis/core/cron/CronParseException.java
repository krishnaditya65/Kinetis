package io.kinetis.core.cron;

/** Thrown when a cron expression string cannot be parsed. */
public class CronParseException extends RuntimeException {

    public CronParseException(String message) {
        super(message);
    }
}
