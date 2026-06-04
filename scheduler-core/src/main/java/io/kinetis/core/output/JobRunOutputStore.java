package io.kinetis.core.output;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Map;
import java.util.UUID;

/**
 * DAO for {@code job_run_outputs} — the result a handler stored via
 * {@link io.kinetis.worker.JobContext#complete}.
 */
public class JobRunOutputStore {

    private final JdbcTemplate jdbc;

    public JobRunOutputStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** Persist the output JSON for a completed run. Idempotent (ON CONFLICT DO NOTHING). */
    public void save(UUID runId, String outputJson) {
        jdbc.update("""
                INSERT INTO job_run_outputs (run_id, output)
                VALUES (?, ?::jsonb)
                ON CONFLICT (run_id) DO UPDATE SET output = EXCLUDED.output
                """, runId, outputJson == null ? "{}" : outputJson);
    }

    /**
     * Fetch outputs from all direct upstream runs of a given downstream run.
     * Returns a map of {@code nodeId → outputJson}; empty if no upstreams have outputs yet.
     */
    public Map<String, String> fetchUpstreamOutputs(UUID downstreamRunId) {
        return jdbc.query("""
                SELECT upstream_run.node_id, COALESCE(o.output::text, '{}') as output
                FROM job_dependencies jd
                JOIN job_runs upstream_run ON upstream_run.id = jd.depends_on_run_id
                LEFT JOIN job_run_outputs o ON o.run_id = jd.depends_on_run_id
                WHERE jd.run_id = ?
                  AND upstream_run.node_id IS NOT NULL
                """,
                rs -> {
                    Map<String, String> result = new java.util.LinkedHashMap<>();
                    while (rs.next()) result.put(rs.getString("node_id"), rs.getString("output"));
                    return result;
                },
                downstreamRunId);
    }
}
