package io.kinetis.core.admin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Moves terminal {@code job_runs} rows older than the configured retention window into
 * {@code job_runs_archive}, keeping the hot table small and index-tight.
 *
 * <p>Archival is safe because only terminal rows (SUCCEEDED, DEAD_LETTER, CANCELLED, SKIPPED)
 * are moved — active rows are never touched. Archived rows remain queryable via
 * {@code SELECT * FROM job_runs_archive}.
 *
 * <p>Runs as a low-priority background task (driven by {@code LoopRunner}) at a configurable
 * interval (default: every hour). Each tick moves at most {@code batchSize} rows to avoid
 * long-running transactions.
 */
public class ArchivalService {

    private static final Logger log = LoggerFactory.getLogger(ArchivalService.class);

    private final JdbcTemplate jdbc;
    private final int retentionDays;
    private final int batchSize;

    public ArchivalService(JdbcTemplate jdbc, int retentionDays, int batchSize) {
        this.jdbc          = jdbc;
        this.retentionDays = retentionDays;
        this.batchSize     = batchSize;
    }

    /**
     * Move one batch of old terminal runs to the archive table.
     *
     * @return number of rows archived in this tick
     */
    public int tick() {
        int archived = jdbc.update("""
                WITH to_archive AS (
                    SELECT id FROM job_runs
                    WHERE state IN ('SUCCEEDED', 'DEAD_LETTER', 'CANCELLED', 'SKIPPED')
                      AND finished_at < now() - make_interval(days => ?)
                    ORDER BY finished_at
                    LIMIT ?
                    FOR UPDATE SKIP LOCKED
                ),
                moved AS (
                    INSERT INTO job_runs_archive
                        SELECT jr.* FROM job_runs jr
                        JOIN to_archive t ON t.id = jr.id
                    ON CONFLICT (id) DO NOTHING
                    RETURNING id
                )
                DELETE FROM job_runs
                WHERE id IN (SELECT id FROM moved)
                """, retentionDays, batchSize);

        if (archived > 0) {
            log.info("archived {} runs older than {} days", archived, retentionDays);
        }
        return archived;
    }

    /**
     * Total archived rows (for metrics / admin endpoint).
     */
    public long archivedCount() {
        Long n = jdbc.queryForObject("SELECT count(*) FROM job_runs_archive", Long.class);
        return n == null ? 0 : n;
    }
}
