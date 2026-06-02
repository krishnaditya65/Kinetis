package io.kinetis.raft;

/**
 * One entry in the Raft replicated log.
 *
 * @param index   1-based position in the log (0 = sentinel before first real entry)
 * @param term    the leader's term when this entry was created
 * @param command opaque byte payload; the {@link RaftStateMachine} interprets it
 */
public record LogEntry(long index, long term, byte[] command) {

    /** Sentinel used as the initial "last log entry" before anything is appended. */
    public static final LogEntry SENTINEL = new LogEntry(0, 0, new byte[0]);
}
