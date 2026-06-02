package io.kinetis.raft.rpc;

/**
 * @param success    true if the follower's log was consistent and entries were appended
 * @param matchIndex the highest log index the follower now has — lets the leader advance
 *                   nextIndex faster than decrementing one at a time
 */
public record AppendEntriesResponse(long term, boolean success, long matchIndex) {}
