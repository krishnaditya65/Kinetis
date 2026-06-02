package io.kinetis.raft;

import io.kinetis.raft.rpc.AppendEntriesRequest;
import io.kinetis.raft.rpc.AppendEntriesResponse;
import io.kinetis.raft.rpc.RaftRpc;
import io.kinetis.raft.rpc.RequestVoteRequest;
import io.kinetis.raft.rpc.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * A single Raft consensus node.
 *
 * <h2>Design</h2>
 * <ul>
 *   <li><b>Single-threaded executor</b> — all state mutations run on one thread. No locking
 *       needed; keeps the algorithm readable.</li>
 *   <li><b>Separate RPC thread pool</b> — AppendEntries and RequestVote are sent concurrently
 *       to all peers (they can block); responses are posted back to the main executor.</li>
 *   <li><b>Randomised election timeout</b> — uniformly in [min, max] to spread out elections
 *       and avoid split votes (Raft §5.2).</li>
 * </ul>
 *
 * <h2>Known gaps (deferred to Phase 6)</h2>
 * Persistent state (currentTerm, votedFor) and joint-consensus membership changes.
 *
 * @see <a href="https://raft.github.io/raft.pdf">Raft paper — Ongaro & Ousterhout (2014)</a>
 */
public class RaftNode {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    // Persistent state (non-durable in this implementation — Phase 6 deferred)
    private long currentTerm = 0;
    private String votedFor  = null;
    private final RaftLog raftLog = new RaftLog();

    // Volatile state
    private RaftState state       = RaftState.FOLLOWER;
    private long commitIndex      = 0;
    private long lastApplied      = 0;
    private String currentLeader  = null;

    // Leader-only state (re-initialised on election)
    private final Map<String, Long> nextIndex  = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();

    // Configuration
    private final String nodeId;
    private final Set<String> peers;
    private final RaftRpc rpc;
    private final RaftStateMachine stateMachine;
    private final long electionTimeoutMinMs;
    private final long electionTimeoutMaxMs;
    private final long heartbeatIntervalMs;

    // Infrastructure
    private final ScheduledExecutorService mainExecutor;
    private final ScheduledExecutorService rpcExecutor;
    private ScheduledFuture<?> electionTimer;
    private final AtomicLong votesReceived = new AtomicLong(0);
    private final List<Consumer<Boolean>> leaderChangeListeners = new ArrayList<>();

    public RaftNode(String nodeId, Set<String> peers, RaftRpc rpc, RaftStateMachine stateMachine,
                    long electionTimeoutMinMs, long electionTimeoutMaxMs, long heartbeatIntervalMs) {
        this.nodeId               = nodeId;
        this.peers                = Set.copyOf(peers);
        this.rpc                  = rpc;
        this.stateMachine         = stateMachine;
        this.electionTimeoutMinMs = electionTimeoutMinMs;
        this.electionTimeoutMaxMs = electionTimeoutMaxMs;
        this.heartbeatIntervalMs  = heartbeatIntervalMs;
        this.mainExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-main-" + nodeId);
            t.setDaemon(true);
            return t;
        });
        this.rpcExecutor = Executors.newScheduledThreadPool(4, r -> {
            Thread t = new Thread(r, "raft-rpc-" + nodeId);
            t.setDaemon(true);
            return t;
        });
    }

    // ---- Public API --------------------------------------------------------

    public void start() {
        resetElectionTimer();
        log.info("[{}] started as FOLLOWER term={}", nodeId, currentTerm);
    }

    public void stop() {
        if (electionTimer != null) electionTimer.cancel(true);
        mainExecutor.shutdownNow();
        rpcExecutor.shutdownNow();
    }

    public RaftState state()      { return state; }
    public long currentTerm()     { return currentTerm; }
    public String currentLeader() { return currentLeader; }
    public boolean isLeader()     { return state == RaftState.LEADER; }
    public String nodeId()        { return nodeId; }

    /** Register a callback invoked on the main executor when leadership changes. */
    public void onLeaderChange(Consumer<Boolean> listener) {
        leaderChangeListeners.add(listener);
    }

    /**
     * Propose a command for replication. Only valid when this node is the leader.
     *
     * @return log index assigned to the entry, or -1 if not leader
     */
    public long propose(byte[] command) {
        if (state != RaftState.LEADER) return -1;
        long index = raftLog.append(currentTerm, command);
        log.debug("[{}] proposed entry at index {}", nodeId, index);
        mainExecutor.execute(this::sendAppendEntriesToAll);
        return index;
    }

    // ---- Inbound RPCs (dispatched to main executor) ------------------------

    public RequestVoteResponse onRequestVote(RequestVoteRequest req) {
        try {
            return mainExecutor.submit(() -> handleRequestVote(req)).get();
        } catch (Exception e) {
            return new RequestVoteResponse(currentTerm, false);
        }
    }

    public AppendEntriesResponse onAppendEntries(AppendEntriesRequest req) {
        try {
            return mainExecutor.submit(() -> handleAppendEntries(req)).get();
        } catch (Exception e) {
            return new AppendEntriesResponse(currentTerm, false, 0);
        }
    }

    // ---- Core algorithm — all methods below run on mainExecutor ------------

    private void electionTimeout() {
        if (state == RaftState.LEADER) return;
        log.info("[{}] election timeout → CANDIDATE term={}", nodeId, currentTerm + 1);
        transitionTo(RaftState.CANDIDATE);
        currentTerm++;
        votedFor = nodeId;
        votesReceived.set(1);

        long lastIndex = raftLog.lastIndex();
        long lastTerm  = raftLog.lastTerm();
        long term      = currentTerm;
        resetElectionTimer();

        for (String peer : peers) {
            rpcExecutor.submit(() -> {
                RequestVoteResponse resp = rpc.requestVote(peer,
                        new RequestVoteRequest(term, nodeId, lastIndex, lastTerm));
                if (resp != null)
                    mainExecutor.execute(() -> handleVoteResponse(peer, term, resp));
            });
        }
    }

    private void handleVoteResponse(String peer, long electionTerm, RequestVoteResponse resp) {
        if (resp.term() > currentTerm) { stepDownToFollower(resp.term(), null); return; }
        if (state != RaftState.CANDIDATE || currentTerm != electionTerm) return;
        if (resp.voteGranted() && votesReceived.incrementAndGet() >= quorum())
            becomeLeader();
    }

    private void becomeLeader() {
        transitionTo(RaftState.LEADER);
        currentLeader = nodeId;
        log.info("[{}] became LEADER term={}", nodeId, currentTerm);
        long nextIdx = raftLog.lastIndex() + 1;
        for (String peer : peers) { nextIndex.put(peer, nextIdx); matchIndex.put(peer, 0L); }
        raftLog.append(currentTerm, new byte[0]); // no-op entry to commit previous-term entries (§8)
        scheduleHeartbeats();
        leaderChangeListeners.forEach(l -> l.accept(true));
    }

    private void scheduleHeartbeats() {
        mainExecutor.scheduleWithFixedDelay(
                () -> { if (state == RaftState.LEADER) sendAppendEntriesToAll(); },
                0, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void sendAppendEntriesToAll() {
        for (String peer : peers) sendAppendEntries(peer);
    }

    private void sendAppendEntries(String peer) {
        long prevIndex = nextIndex.getOrDefault(peer, raftLog.lastIndex() + 1) - 1;
        long prevTerm  = raftLog.termAt(prevIndex);
        List<LogEntry> entries = raftLog.from(prevIndex + 1);
        long ci = commitIndex;
        long term = currentTerm;
        AppendEntriesRequest req = new AppendEntriesRequest(term, nodeId, prevIndex, prevTerm, entries, ci);
        rpcExecutor.submit(() -> {
            AppendEntriesResponse resp = rpc.appendEntries(peer, req);
            if (resp != null)
                mainExecutor.execute(() -> handleAppendEntriesResponse(peer, term, prevIndex, entries, resp));
        });
    }

    private void handleAppendEntriesResponse(String peer, long sentTerm, long prevIndex,
                                              List<LogEntry> sentEntries, AppendEntriesResponse resp) {
        if (resp.term() > currentTerm) { stepDownToFollower(resp.term(), null); return; }
        if (state != RaftState.LEADER || currentTerm != sentTerm) return;
        if (resp.success()) {
            long newMatch = resp.matchIndex() > 0 ? resp.matchIndex() : prevIndex + sentEntries.size();
            matchIndex.merge(peer, newMatch, Math::max);
            nextIndex.put(peer, newMatch + 1);
            tryAdvanceCommitIndex();
        } else {
            nextIndex.merge(peer, -1L,
                    (cur, delta) -> Math.max(raftLog.snapshotIndex() + 1, cur + delta));
        }
    }

    private void tryAdvanceCommitIndex() {
        for (long n = raftLog.lastIndex(); n > commitIndex; n--) {
            if (raftLog.termAt(n) != currentTerm) continue;
            long count = 1;
            for (long match : matchIndex.values()) if (match >= n) count++;
            if (count >= quorum()) { commitIndex = n; applyCommitted(); break; }
        }
    }

    private RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
        if (req.term() > currentTerm) stepDownToFollower(req.term(), null);
        if (req.term() < currentTerm) return new RequestVoteResponse(currentTerm, false);
        boolean canVote = (votedFor == null || votedFor.equals(req.candidateId()))
                && isAtLeastAsUpToDate(req.lastLogIndex(), req.lastLogTerm());
        if (canVote) {
            votedFor = req.candidateId();
            resetElectionTimer();
            return new RequestVoteResponse(currentTerm, true);
        }
        return new RequestVoteResponse(currentTerm, false);
    }

    private AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
        if (req.term() < currentTerm) return new AppendEntriesResponse(currentTerm, false, 0);
        if (req.term() > currentTerm) stepDownToFollower(req.term(), req.leaderId());
        currentLeader = req.leaderId();
        resetElectionTimer();

        if (req.prevLogIndex() > 0 && raftLog.termAt(req.prevLogIndex()) != req.prevLogTerm())
            return new AppendEntriesResponse(currentTerm, false, 0);

        if (!req.entries().isEmpty())
            raftLog.replaceFrom(req.prevLogIndex() + 1, req.entries());

        if (req.leaderCommit() > commitIndex) {
            commitIndex = Math.min(req.leaderCommit(), raftLog.lastIndex());
            applyCommitted();
        }
        return new AppendEntriesResponse(currentTerm, true, raftLog.lastIndex());
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            LogEntry entry = raftLog.get(lastApplied);
            if (entry != null && entry.command().length > 0)
                stateMachine.apply(lastApplied, entry.command());
        }
    }

    private void stepDownToFollower(long newTerm, String leaderId) {
        boolean wasLeader = (state == RaftState.LEADER);
        currentTerm   = newTerm;
        votedFor      = null;
        currentLeader = leaderId;
        transitionTo(RaftState.FOLLOWER);
        if (wasLeader) leaderChangeListeners.forEach(l -> l.accept(false));
        resetElectionTimer();
    }

    private void transitionTo(RaftState newState) {
        if (state != newState) { log.debug("[{}] {} → {}", nodeId, state, newState); state = newState; }
    }

    private void resetElectionTimer() {
        if (electionTimer != null) electionTimer.cancel(false);
        long timeout = electionTimeoutMinMs + ThreadLocalRandom.current()
                .nextLong(electionTimeoutMaxMs - electionTimeoutMinMs);
        electionTimer = mainExecutor.schedule(this::electionTimeout, timeout, TimeUnit.MILLISECONDS);
    }

    private boolean isAtLeastAsUpToDate(long candidateLastIndex, long candidateLastTerm) {
        long myLastTerm = raftLog.lastTerm(), myLastIndex = raftLog.lastIndex();
        if (candidateLastTerm != myLastTerm) return candidateLastTerm > myLastTerm;
        return candidateLastIndex >= myLastIndex;
    }

    /** Majority quorum: floor((peers + 1) / 2) + 1 */
    private int quorum() { return (peers.size() + 1) / 2 + 1; }

    @Override
    public String toString() { return "RaftNode[" + nodeId + "/" + state + "/term=" + currentTerm + "]"; }
}
