package io.kinetis.raft.rpc;

import io.kinetis.raft.RaftNode;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-process RPC implementation for tests and local demos. All nodes share a registry and
 * calls are direct method invocations — no network. Returns null for unknown peer IDs,
 * simulating an unreachable node.
 */
public class InMemoryRaftRpc implements RaftRpc {

    private final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();

    public void register(RaftNode node) {
        nodes.put(node.nodeId(), node);
    }

    @Override
    public RequestVoteResponse requestVote(String peerId, RequestVoteRequest request) {
        RaftNode peer = nodes.get(peerId);
        return peer == null ? null : peer.onRequestVote(request);
    }

    @Override
    public AppendEntriesResponse appendEntries(String peerId, AppendEntriesRequest request) {
        RaftNode peer = nodes.get(peerId);
        return peer == null ? null : peer.onAppendEntries(request);
    }
}
