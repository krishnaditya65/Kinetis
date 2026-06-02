package io.kinetis.raft;

/** The three roles a Raft node cycles through. */
public enum RaftState {
    /** Default state. Follows the leader; resets election timer on valid heartbeats. */
    FOLLOWER,
    /** Timed out with no heartbeat; soliciting votes to become leader. */
    CANDIDATE,
    /** Won majority of votes; sending heartbeats and replicating log entries. */
    LEADER
}
