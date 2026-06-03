package io.kinetis.api.security;

import jakarta.servlet.http.HttpServletRequest;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * AOP aspect that fires an audit event after every successful write on the REST API.
 * Fires after-returning (not after-throwing) so only successful operations are logged.
 */
@Aspect
@Component
public class AuditAspect {

    private final AuditLog auditLog;

    public AuditAspect(AuditLog auditLog) {
        this.auditLog = auditLog;
    }

    @AfterReturning("execution(* io.kinetis.api.web.JobController.submit(..))")
    public void onJobSubmit(JoinPoint jp) {
        audit("submit_job", extractFirstArg(jp));
    }

    @AfterReturning("execution(* io.kinetis.api.web.JobController.cancel(..))")
    public void onJobCancel(JoinPoint jp) {
        audit("cancel_job", extractFirstArg(jp));
    }

    @AfterReturning("execution(* io.kinetis.api.workflow.WorkflowController.submit(..))")
    public void onWorkflowSubmit(JoinPoint jp) {
        audit("submit_workflow", null);
    }

    @AfterReturning("execution(* io.kinetis.api.workflow.WorkflowController.cancel(..))")
    public void onWorkflowCancel(JoinPoint jp) {
        audit("cancel_workflow", extractFirstArg(jp));
    }

    private void audit(String action, Object resourceId) {
        String actor = resolveActor();
        String rid   = resourceId == null ? null : resourceId.toString();
        auditLog.record(actor, action, rid, null);
    }

    private static String resolveActor() {
        try {
            ServletRequestAttributes attrs =
                    (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
            if (attrs == null) return "anonymous";
            HttpServletRequest req = attrs.getRequest();
            String key = req.getHeader("X-Api-Key");
            return key == null || key.isBlank() ? "anonymous" : "key:" + key.substring(0, 8) + "…";
        } catch (Exception e) {
            return "anonymous";
        }
    }

    private static Object extractFirstArg(JoinPoint jp) {
        Object[] args = jp.getArgs();
        return args.length > 0 ? args[0] : null;
    }
}
