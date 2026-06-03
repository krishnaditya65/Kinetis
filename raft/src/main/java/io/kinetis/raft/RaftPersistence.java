package io.kinetis.raft;

/**
 * Durable storage for the two Raft state fields that MUST survive process restarts:
 *
 * <ul>
 *   <li>{@code currentTerm} — a node that forgets its term could accept a stale leader
 *       or vote for a candidate in a term it already voted in, breaking election safety.</li>
 *   <li>{@code votedFor} — a node that forgets its vote could cast two votes in the same
 *       term for different candidates, potentially electing two leaders simultaneously.</li>
 * </ul>
 *
 * <p>Both fields must be flushed to stable storage <em>before</em> responding to any RPC
 * that changes them (Raft §5.4 "Persistence").
 *
 * <p>The log itself is not persisted here — that defers to a future phase. Without log
 * persistence a restarted node rejoins as a fresh follower and receives a full snapshot
 * + log replay from the leader, which is safe (slow but correct).
 */
public interface RaftPersistence {

    /** Read previously persisted state. Returns defaults (term=0, votedFor=null) on first run. */
    PersistedState load();

    /** Flush updated state to stable storage. Must complete before the caller returns. */
    void save(long currentTerm, String votedFor);

    record PersistedState(long currentTerm, String votedFor) {
        public static PersistedState initial() {
            return new PersistedState(0L, null);
        }
    }
}
