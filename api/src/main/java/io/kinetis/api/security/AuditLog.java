package io.kinetis.api.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * Persists audit events asynchronously. Write failures are logged but never propagated —
 * an audit log issue must never break the primary API path.
 */
@Component
public class AuditLog {

    private static final Logger log = LoggerFactory.getLogger(AuditLog.class);

    private final JdbcTemplate jdbc;

    public AuditLog(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Async
    public void record(String actor, String action, String resourceId, String detailJson) {
        try {
            jdbc.update("""
                    INSERT INTO audit_events (actor, action, resource_id, detail)
                    VALUES (?, ?, ?, ?::jsonb)
                    """, actor, action, resourceId,
                    detailJson == null ? "{}" : detailJson);
        } catch (Exception e) {
            log.warn("audit log write failed: actor={} action={} resource={}", actor, action, resourceId, e);
        }
    }
}
