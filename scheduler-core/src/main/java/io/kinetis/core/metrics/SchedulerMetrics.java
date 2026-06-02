package io.kinetis.core.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Thin wrapper over Micrometer counters for the scheduler's key lifecycle events.
 * Each counter maps to a Prometheus metric scraped via the actuator endpoint.
 */
public class SchedulerMetrics {

    private final Counter submitted;
    private final Counter leased;
    private final Counter succeeded;
    private final Counter failed;
    private final Counter retried;
    private final Counter deadLettered;
    private final Counter reaped;

    public SchedulerMetrics(MeterRegistry registry) {
        this.submitted    = Counter.builder("kinetis.jobs.submitted").register(registry);
        this.leased       = Counter.builder("kinetis.runs.leased").register(registry);
        this.succeeded    = Counter.builder("kinetis.runs.succeeded").register(registry);
        this.failed       = Counter.builder("kinetis.runs.failed").register(registry);
        this.retried      = Counter.builder("kinetis.runs.retried").register(registry);
        this.deadLettered = Counter.builder("kinetis.runs.dead_lettered").register(registry);
        this.reaped       = Counter.builder("kinetis.runs.reaped").register(registry);
    }

    public void onSubmitted()      { submitted.increment(); }
    public void onLeased(int n)    { leased.increment(n); }
    public void onSucceeded()      { succeeded.increment(); }
    public void onFailed()         { failed.increment(); }
    public void onRetried()        { retried.increment(); }
    public void onDeadLettered()   { deadLettered.increment(); }
    public void onReaped(int n)    { reaped.increment(n); }
}
