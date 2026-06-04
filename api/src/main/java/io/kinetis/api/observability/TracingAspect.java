package io.kinetis.api.observability;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.kinetis.core.model.JobRun;
import io.kinetis.core.service.SubmitCommand;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * AOP aspect that wraps key scheduler operations with Micrometer Observations, which are
 * automatically exported as OpenTelemetry spans via the micrometer-tracing-bridge-otel bridge.
 *
 * <h2>Instrumented paths</h2>
 * <ul>
 *   <li>{@code kinetis.job.submit} — job submission with jobType tag</li>
 *   <li>{@code kinetis.scheduler.tick} — one scheduler poll cycle; records batch size</li>
 *   <li>{@code kinetis.worker.execute} — full job execution span (most valuable for latency)</li>
 *   <li>{@code kinetis.workflow.advance} — one WorkflowAdvancer tick</li>
 * </ul>
 *
 * <p>Sampling rate is controlled by {@code management.tracing.sampling.probability} in
 * {@code application.yml} (default 10%). Set to {@code 1.0} in development.
 */
@Aspect
@Component
public class TracingAspect {

    private final ObservationRegistry registry;

    public TracingAspect(ObservationRegistry registry) {
        this.registry = registry;
    }

    @Around("execution(* io.kinetis.core.service.JobService.submit(..))")
    public Object traceJobSubmit(ProceedingJoinPoint pjp) throws Throwable {
        Object[] args = pjp.getArgs();
        String jobType = args.length > 0 && args[0] instanceof SubmitCommand cmd
                ? cmd.jobType() : "unknown";

        return Observation.createNotStarted("kinetis.job.submit", registry)
                .lowCardinalityKeyValue("jobType", jobType)
                .observe(() -> {
                    try { return pjp.proceed(); }
                    catch (Throwable t) { throw new RuntimeException(t); }
                });
    }

    @Around("execution(* io.kinetis.core.scheduler.SchedulerLoop.tick(..))")
    public Object traceSchedulerTick(ProceedingJoinPoint pjp) throws Throwable {
        return Observation.createNotStarted("kinetis.scheduler.tick", registry)
                .observe(() -> {
                    try { return pjp.proceed(); }
                    catch (Throwable t) { throw new RuntimeException(t); }
                });
    }

    @Around("execution(* io.kinetis.worker.WorkerPool.dispatch(..))")
    public Object traceWorkerDispatch(ProceedingJoinPoint pjp) throws Throwable {
        Object[] args = pjp.getArgs();
        String runId = args.length > 0 && args[0] instanceof JobRun run
                ? run.id().toString().substring(0, 8) : "?";

        return Observation.createNotStarted("kinetis.worker.dispatch", registry)
                .lowCardinalityKeyValue("runId.prefix", runId)
                .observe(() -> {
                    try { return pjp.proceed(); }
                    catch (Throwable t) { throw new RuntimeException(t); }
                });
    }

    @Around("execution(* io.kinetis.core.workflow.WorkflowAdvancer.tick(..))")
    public Object traceWorkflowAdvance(ProceedingJoinPoint pjp) throws Throwable {
        return Observation.createNotStarted("kinetis.workflow.advance", registry)
                .observe(() -> {
                    try { return pjp.proceed(); }
                    catch (Throwable t) { throw new RuntimeException(t); }
                });
    }
}
