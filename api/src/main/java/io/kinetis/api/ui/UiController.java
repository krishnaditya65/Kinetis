package io.kinetis.api.ui;

import io.kinetis.core.model.JobState;
import io.kinetis.core.service.JobService;
import io.kinetis.core.workflow.WorkflowService;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Server-side rendered UI controller. Pages use HTMX for live updates without a full SPA build.
 * All data is fetched from existing service/store beans — no separate data layer.
 */
@Controller
@RequestMapping("/ui")
public class UiController {

    private final JobService jobService;
    private final WorkflowService workflowService;
    private final JdbcTemplate jdbc;

    public UiController(JobService jobService, WorkflowService workflowService,
                        JdbcTemplate jdbc) {
        this.jobService      = jobService;
        this.workflowService = workflowService;
        this.jdbc            = jdbc;
    }

    /** Job browser — filterable, paginated list of recent job_runs. */
    @GetMapping("/jobs")
    public String jobs(@RequestParam(required = false) String state,
                       @RequestParam(required = false) String jobType,
                       @RequestParam(required = false) String tenantId,
                       @RequestParam(defaultValue = "0") int page,
                       Model model) {
        int pageSize = 50;
        int offset   = page * pageSize;

        // Build a flexible query based on optional filters
        StringBuilder sql = new StringBuilder("""
                SELECT jr.id, jr.state, jr.attempt, jr.priority, jr.tenant_id,
                       jr.scheduled_for, jr.started_at, jr.finished_at, jr.last_error,
                       j.job_type, j.id as job_id
                FROM job_runs jr
                JOIN jobs j ON j.id = jr.job_id
                WHERE 1=1
                """);
        List<Object> params = new java.util.ArrayList<>();

        if (state != null && !state.isBlank()) {
            sql.append(" AND jr.state = ?"); params.add(state);
        }
        if (jobType != null && !jobType.isBlank()) {
            sql.append(" AND j.job_type = ?"); params.add(jobType);
        }
        if (tenantId != null && !tenantId.isBlank()) {
            sql.append(" AND jr.tenant_id = ?"); params.add(tenantId);
        }
        sql.append(" ORDER BY jr.enqueued_at DESC LIMIT ? OFFSET ?");
        params.add(pageSize); params.add(offset);

        List<Map<String, Object>> runs = jdbc.queryForList(sql.toString(), params.toArray());

        model.addAttribute("runs",     runs);
        model.addAttribute("states",   JobState.values());
        model.addAttribute("filter",   Map.of(
                "state",    state    == null ? "" : state,
                "jobType",  jobType  == null ? "" : jobType,
                "tenantId", tenantId == null ? "" : tenantId));
        model.addAttribute("page",     page);
        model.addAttribute("hasMore",  runs.size() == pageSize);
        return "jobs";
    }

    /** Job detail — all runs for a single job. */
    @GetMapping("/jobs/{jobId}")
    public String jobDetail(@PathVariable UUID jobId, Model model) {
        var job = jobService.findJob(jobId)
                .orElseThrow(() -> new org.springframework.web.server.ResponseStatusException(
                        org.springframework.http.HttpStatus.NOT_FOUND));
        var runs = jobService.findRuns(jobId);
        model.addAttribute("job",  job);
        model.addAttribute("runs", runs);
        return "job-detail";
    }

    /** DAG visualiser — nodes + edges from a workflow. */
    @GetMapping("/workflows/{workflowId}")
    public String workflowDetail(@PathVariable UUID workflowId, Model model) {
        var wf = workflowService.findWorkflow(workflowId)
                .orElseThrow(() -> new org.springframework.web.server.ResponseStatusException(
                        org.springframework.http.HttpStatus.NOT_FOUND));
        var nodes = workflowService.findNodes(workflowId);

        // Load edges for visualisation
        List<Map<String, Object>> edges = jdbc.queryForList("""
                SELECT jd.depends_on_run_id as from_run, jd.run_id as to_run,
                       from_r.node_id as from_node, to_r.node_id as to_node
                FROM job_dependencies jd
                JOIN job_runs from_r ON from_r.id = jd.depends_on_run_id
                JOIN job_runs to_r   ON to_r.id   = jd.run_id
                WHERE from_r.workflow_id = ?
                """, workflowId);

        model.addAttribute("workflow", wf);
        model.addAttribute("nodes",    nodes);
        model.addAttribute("edges",    edges);
        return "workflow";
    }
}
