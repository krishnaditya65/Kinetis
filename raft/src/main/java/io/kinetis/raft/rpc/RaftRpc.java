package io.kinetis.raft.rpc;

/**
 * Transport abstraction for Raft RPCs. {@link io.kinetis.raft.RaftNode} never touches the
 * network directly — it calls this interface, keeping consensus code transport-agnostic and
 * fully testable in-process.
 *
 * <p>Implementations: {@link InMemoryRaftRpc} (tests), gRPC (production).
 * All methods are synchronous from the caller's perspective; return null on timeout/unreachable.
 */
public interface RaftRpc {

    /** Send RequestVote to {@code peerId}. Returns null if unreachable. */
    RequestVoteResponse requestVote(String peerId, RequestVoteRequest request);

    /** Send AppendEntries (or heartbeat when entries is empty). Returns null on timeout. */
    AppendEntriesResponse appendEntries(String peerId, AppendEntriesRequest request);
}
