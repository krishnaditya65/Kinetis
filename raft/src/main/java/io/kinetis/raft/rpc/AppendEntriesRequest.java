package io.kinetis.raft.rpc;

import io.kinetis.raft.LogEntry;

import java.util.List;

/**
 * §5.3 — Leader replicates log entries. Empty {@code entries} = heartbeat.
 *
 * @param prevLogIndex index of the entry immediately before the new entries
 * @param prevLogTerm  term of that preceding entry (consistency check)
 * @param leaderCommit the leader's current commitIndex
 */
public record AppendEntriesRequest(
        long term,
        String leaderId,
        long prevLogIndex,
        long prevLogTerm,
        List<LogEntry> entries,
        long leaderCommit
) {}
