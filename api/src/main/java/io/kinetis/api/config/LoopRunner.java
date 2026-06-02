package io.kinetis.api.config;

import io.kinetis.core.cron.CronScheduler;
import io.kinetis.core.queue.RateLimiter;
import io.kinetis.core.reaper.ReaperLoop;
import io.kinetis.core.scheduler.SchedulerLoop;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Drives the scheduler, reaper, cron, and rate-limit-refill loops on fixed delays.
 * Uses a dedicated executor so cadence is controlled by {@link SchedulerProperties} typed
 * Durations rather than annotation strings. Each tick swallows its own errors so a transient
 * DB blip never cancels the recurring task.
 *
 * <p>In {@code app.role=worker} mode the loops are skipped — the worker process only handles
 * inbound gRPC dispatches, not scheduling.
 */
@Component
public class LoopRunner implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LoopRunner.class);

    private final SchedulerLoop schedulerLoop;
    private final ReaperLoop reaperLoop;
    private final CronScheduler cronScheduler;
    private final RateLimiter rateLimiter;
    private final SchedulerProperties props;
    private final String role;

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(4, r -> {
        Thread t = new Thread(r, "scheduler-loop");
        t.setDaemon(true);
        return t;
    });

    public LoopRunner(SchedulerLoop schedulerLoop, ReaperLoop reaperLoop,
                      CronScheduler cronScheduler, RateLimiter rateLimiter,
                      SchedulerProperties props,
                      @Value("${app.role:standalone}") String role) {
        this.schedulerLoop = schedulerLoop;
        this.reaperLoop    = reaperLoop;
        this.cronScheduler = cronScheduler;
        this.rateLimiter   = rateLimiter;
        this.props         = props;
        this.role          = role;
    }

    @PostConstruct
    public void start() {
        if ("worker".equals(role)) {
            log.info("LoopRunner: role=worker — scheduler/reaper/cron/refill loops disabled");
            return;
        }
        long pollMs   = props.getPollInterval().toMillis();
        long reapMs   = props.getReaperInterval().toMillis();
        long cronMs   = props.getCronInterval().toMillis();
        long refillMs = props.getRateLimitRefillInterval().toMillis();

        executor.scheduleWithFixedDelay(() -> guard("scheduler", schedulerLoop::tick),    pollMs,   pollMs,   TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(() -> guard("reaper",    reaperLoop::tick),       reapMs,   reapMs,   TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(() -> guard("cron",      cronScheduler::tick),    cronMs,   cronMs,   TimeUnit.MILLISECONDS);
        executor.scheduleWithFixedDelay(() -> guard("refill",    rateLimiter::refillAll), refillMs, refillMs, TimeUnit.MILLISECONDS);

        log.info("LoopRunner started (role={}): poll={}ms reaper={}ms cron={}ms refill={}ms",
                role, pollMs, reapMs, cronMs, refillMs);
    }

    private void guard(String name, Runnable task) {
        try {
            task.run();
        } catch (Throwable t) {
            if (Thread.currentThread().isInterrupted() || hasInterruptedCause(t)) return;
            log.warn("{} tick threw", name, t);
        }
    }

    private static boolean hasInterruptedCause(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause())
            if (c instanceof InterruptedException) return true;
        return false;
    }

    @PreDestroy
    @Override
    public void close() {
        executor.shutdownNow();
    }
}
