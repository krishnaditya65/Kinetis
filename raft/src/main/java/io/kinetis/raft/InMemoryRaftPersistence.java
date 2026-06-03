package io.kinetis.raft;

/**
 * In-memory (non-durable) persistence — for tests and demos only.
 * State is lost on process restart; do NOT use in production.
 */
public class InMemoryRaftPersistence implements RaftPersistence {

    private long   currentTerm = 0;
    private String votedFor    = null;

    @Override
    public PersistedState load() {
        return new PersistedState(currentTerm, votedFor);
    }

    @Override
    public void save(long term, String voted) {
        this.currentTerm = term;
        this.votedFor    = voted;
    }
}
