package io.kinetis.core.workflow;

/**
 * What to do when a node in a DAG workflow fails.
 *
 * <ul>
 *   <li>{@link #FAIL_FAST} — cancel all non-terminal nodes immediately. Default.</li>
 *   <li>{@link #WAIT} — let other branches run; the failed branch's dependents stay
 *       PENDING_DEPS until manual intervention.</li>
 *   <li>{@link #SKIP_DOWNSTREAM} — mark the failed node's direct dependents SKIPPED and
 *       continue the rest of the DAG unaffected.</li>
 * </ul>
 */
public enum FailurePolicy {
    FAIL_FAST,
    WAIT,
    SKIP_DOWNSTREAM
}
