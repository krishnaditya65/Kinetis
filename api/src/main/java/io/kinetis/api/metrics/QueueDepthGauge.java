package io.kinetis.api.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/** Publishes "how many runs are due and waiting" as a Micrometer gauge. */
@Component
public class QueueDepthGauge {

    private final MeterRegistry registry;
    private final JdbcTemplate jdbc;

    public QueueDepthGauge(MeterRegistry registry, JdbcTemplate jdbc) {
        this.registry = registry;
        this.jdbc     = jdbc;
    }

    @PostConstruct
    void register() {
        Gauge.builder("kinetis.runs.scheduled_depth", this, QueueDepthGauge::dueCount)
                .description("Runs that are due (scheduled_for <= now) and not yet leased")
                .register(registry);
    }

    private double dueCount() {
        try {
            Long n = jdbc.queryForObject(
                    "SELECT count(*) FROM job_runs WHERE state = 'SCHEDULED' AND scheduled_for <= now()",
                    Long.class);
            return n == null ? 0.0 : n.doubleValue();
        } catch (RuntimeException e) {
            return 0.0;
        }
    }
}
