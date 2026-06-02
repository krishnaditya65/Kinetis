package io.kinetis.raft.rpc;

/** §5.2 — Candidate asks a peer for a vote. */
public record RequestVoteRequest(
        long term,
        String candidateId,
        long lastLogIndex,
        long lastLogTerm
) {}
