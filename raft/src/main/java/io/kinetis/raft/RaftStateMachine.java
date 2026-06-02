package io.kinetis.raft;

/**
 * Application state machine driven by committed Raft log entries. Implementations must be
 * deterministic: given the same sequence of commands in the same order, every node reaches
 * the same state — this is the core Raft correctness property.
 */
public interface RaftStateMachine {

    /**
     * Apply a committed command. Called by {@link RaftNode} in index order, single-threaded.
     *
     * @param index   log index of this entry (for idempotency checks if needed)
     * @param command opaque byte payload from the log entry
     */
    void apply(long index, byte[] command);

    /** Return a snapshot of current state (for log compaction). */
    byte[] snapshot();

    /**
     * Restore from a snapshot produced by a peer's {@link #snapshot()}.
     * Called when installing a snapshot from the leader during log compaction catch-up.
     */
    void restoreSnapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshot);
}
