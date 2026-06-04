package io.kinetis.core.admin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Maintenance mode flag. When enabled, the scheduler, reaper, and cron loops skip their ticks
 * so the DB can be drained, migrated, or resized without new work being claimed.
 *
 * <p>State is persisted in {@code scheduler_settings} so it survives restarts (useful for
 * coordinated rolling deploys — set maintenance on, wait for all nodes to drain, deploy).
 * In-memory {@link AtomicBoolean} is authoritative for the current process; the DB copy is
 * loaded on startup and synced on every set().
 */
public class MaintenanceFlag {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceFlag.class);
    private static final String KEY = "maintenance_mode";

    private final JdbcTemplate jdbc;
    private final AtomicBoolean enabled;

    public MaintenanceFlag(JdbcTemplate jdbc) {
        this.jdbc    = jdbc;
        this.enabled = new AtomicBoolean(loadFromDb());
        log.info("MaintenanceFlag initialised: enabled={}", enabled.get());
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public void enable() {
        enabled.set(true);
        persist(true);
        log.warn("MAINTENANCE MODE ENABLED — scheduler loops paused");
    }

    public void disable() {
        enabled.set(false);
        persist(false);
        log.info("Maintenance mode disabled — scheduler loops resumed");
    }

    private boolean loadFromDb() {
        try {
            String val = jdbc.queryForObject(
                    "SELECT value FROM scheduler_settings WHERE key = ?", String.class, KEY);
            return "true".equalsIgnoreCase(val);
        } catch (Exception e) {
            log.debug("could not read maintenance_mode from DB; defaulting to false", e);
            return false;
        }
    }

    private void persist(boolean value) {
        try {
            jdbc.update("""
                    INSERT INTO scheduler_settings (key, value, updated_at)
                    VALUES (?, ?, now())
                    ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()
                    """, KEY, String.valueOf(value));
        } catch (Exception e) {
            log.warn("failed to persist maintenance mode to DB", e);
        }
    }
}
