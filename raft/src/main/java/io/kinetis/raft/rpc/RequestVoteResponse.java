package io.kinetis.raft.rpc;

public record RequestVoteResponse(long term, boolean voteGranted) {}
