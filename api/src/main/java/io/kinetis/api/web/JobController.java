package io.kinetis.api.web;

import com.fasterxml.jackson.databind.JsonNode;
import io.kinetis.api.dto.JobView;
import io.kinetis.api.dto.RunView;
import io.kinetis.api.dto.SubmitJobRequest;
import io.kinetis.api.dto.SubmitJobResponse;
import io.kinetis.core.metrics.SchedulerMetrics;
import io.kinetis.core.model.RetryPolicy;
import io.kinetis.core.service.JobService;
import io.kinetis.core.service.JobSubmission;
import io.kinetis.core.service.SubmitCommand;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/jobs")
public class JobController {

    private final JobService jobService;
    private final SchedulerMetrics metrics;
    private final Clock clock;

    public JobController(JobService jobService, SchedulerMetrics metrics, Clock clock) {
        this.jobService = jobService;
        this.metrics    = metrics;
        this.clock      = clock;
    }

    @PostMapping
    public ResponseEntity<SubmitJobResponse> submit(@Valid @RequestBody SubmitJobRequest req) {
        SubmitCommand cmd = new SubmitCommand(
                req.jobType(),
                payloadJson(req.payload()),
                req.idempotencyKey(),
                req.deliveryPolicy(),
                parseScheduleAt(req.scheduleAt(), clock.instant()),
                req.cronExpr(),
                req.timezone(),
                req.misfirePolicy(),
                retryPolicy(req),
                req.priority() != null ? req.priority() : 0,
                req.tenantId(),
                req.callbackUrl());

        JobSubmission submission = jobService.submit(cmd);
        if (submission.created()) metrics.onSubmitted();
        HttpStatus status = submission.created() ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(status).body(SubmitJobResponse.from(submission));
    }

    @GetMapping("/{jobId}")
    public JobView getJob(@PathVariable UUID jobId) {
        return jobService.findJob(jobId).map(JobView::from)
                .orElseThrow(() -> notFound("job", jobId));
    }

    @GetMapping("/{jobId}/runs")
    public List<RunView> getRuns(@PathVariable UUID jobId) {
        jobService.findJob(jobId).orElseThrow(() -> notFound("job", jobId));
        return jobService.findRuns(jobId).stream().map(RunView::from).toList();
    }

    @GetMapping("/{jobId}/next-fire")
    public Map<String, Object> nextFire(@PathVariable UUID jobId) {
        jobService.findJob(jobId).orElseThrow(() -> notFound("job", jobId));
        return jobService.nextFireTime(jobId)
                .<Map<String, Object>>map(t -> Map.of("jobId", jobId, "nextFireTime", t))
                .orElse(Map.of("jobId", jobId, "nextFireTime", "null",
                        "note", "one-off job or cron disabled"));
    }

    @DeleteMapping("/{jobId}")
    public Map<String, Object> cancel(@PathVariable UUID jobId) {
        jobService.findJob(jobId).orElseThrow(() -> notFound("job", jobId));
        int cancelled = jobService.cancel(jobId);
        return Map.of("jobId", jobId, "cancelledRuns", cancelled);
    }

    // ---- helpers ----

    private static String payloadJson(JsonNode payload) {
        return payload == null || payload.isNull() ? "{}" : payload.toString();
    }

    private static RetryPolicy retryPolicy(SubmitJobRequest req) {
        if (req.maxAttempts() == null && req.backoffBaseMs() == null && req.backoffFactor() == null)
            return null;
        RetryPolicy d = RetryPolicy.defaults();
        return new RetryPolicy(
                req.maxAttempts()    != null ? req.maxAttempts()    : d.maxAttempts(),
                req.backoffBaseMs()  != null ? req.backoffBaseMs()  : d.backoffBaseMs(),
                req.backoffFactor()  != null ? req.backoffFactor()  : d.backoffFactor());
    }

    /**
     * Parse schedule time: null/blank → now; {@code +5s}/{@code +2m}/{@code +1h} → now + offset;
     * otherwise ISO-8601 instant.
     */
    static Instant parseScheduleAt(String raw, Instant now) {
        if (raw == null || raw.isBlank()) return now;
        String s = raw.trim();
        if (s.startsWith("+")) return now.plus(parseOffset(s.substring(1)));
        try {
            return Instant.parse(s);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(
                    "scheduleAt must be an ISO-8601 instant or a relative offset like +5s: " + raw);
        }
    }

    private static Duration parseOffset(String offset) {
        int i = 0;
        while (i < offset.length() && Character.isDigit(offset.charAt(i))) i++;
        if (i == 0) throw new IllegalArgumentException("invalid offset: +" + offset);
        long value = Long.parseLong(offset.substring(0, i));
        return switch (offset.substring(i).toLowerCase()) {
            case "ms"    -> Duration.ofMillis(value);
            case "s", "" -> Duration.ofSeconds(value);
            case "m"     -> Duration.ofMinutes(value);
            case "h"     -> Duration.ofHours(value);
            case "d"     -> Duration.ofDays(value);
            default -> throw new IllegalArgumentException(
                    "unknown time unit '" + offset.substring(i) + "' in offset +" + offset);
        };
    }

    private static ResponseStatusException notFound(String kind, UUID id) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, kind + " not found: " + id);
    }
}
