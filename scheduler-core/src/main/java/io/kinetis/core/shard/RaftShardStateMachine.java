package io.kinetis.core.shard;

import io.kinetis.raft.RaftStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Raft state machine for shard assignment. Commands are UTF-8 strings:
 * <pre>
 *   ASSIGN  nodeId shardId   — assign shard to node
 *   RELEASE nodeId shardId   — release shard from node
 * </pre>
 * Every node runs this state machine; the Raft log guarantees all nodes converge to the same
 * assignment after the same sequence of committed commands. Reads are lock-free via
 * {@link CopyOnWriteArraySet}.
 */
public class RaftShardStateMachine implements RaftStateMachine {

    private static final Logger log = LoggerFactory.getLogger(RaftShardStateMachine.class);

    private final String localNodeId;
    private final CopyOnWriteArraySet<Integer> ownedShards = new CopyOnWriteArraySet<>();

    public RaftShardStateMachine(String localNodeId) {
        this.localNodeId = localNodeId;
    }

    @Override
    public void apply(long index, byte[] command) {
        String cmd = new String(command, StandardCharsets.UTF_8).trim();
        String[] parts = cmd.split("\\s+");
        if (parts.length != 3) {
            log.warn("malformed shard command at index {}: '{}'", index, cmd);
            return;
        }
        String op     = parts[0];
        String nodeId = parts[1];
        int shardId;
        try {
            shardId = Integer.parseInt(parts[2]);
        } catch (NumberFormatException e) {
            log.warn("bad shard id in command at index {}: '{}'", index, cmd);
            return;
        }
        if (!localNodeId.equals(nodeId)) return;
        switch (op) {
            case "ASSIGN"  -> { ownedShards.add(shardId);    log.info("shard {} ASSIGNED to {}",  shardId, nodeId); }
            case "RELEASE" -> { ownedShards.remove(shardId); log.info("shard {} RELEASED from {}", shardId, nodeId); }
            default        -> log.warn("unknown shard op '{}' at index {}", op, index);
        }
    }

    @Override
    public byte[] snapshot() {
        String data = String.join(" ", ownedShards.stream().sorted().map(String::valueOf).toList());
        return (localNodeId + "=" + data).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void restoreSnapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] snapshot) {
        String s  = new String(snapshot, StandardCharsets.UTF_8);
        int eq    = s.indexOf('=');
        if (eq < 0 || !localNodeId.equals(s.substring(0, eq))) return;
        String shards = s.substring(eq + 1).trim();
        ownedShards.clear();
        if (!shards.isEmpty())
            Arrays.stream(shards.split("\\s+")).map(Integer::parseInt).forEach(ownedShards::add);
    }

    public Set<Integer> ownedShards() {
        return Collections.unmodifiableSet(new HashSet<>(ownedShards));
    }

    public static byte[] assignCommand(String nodeId, int shardId) {
        return ("ASSIGN "  + nodeId + " " + shardId).getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] releaseCommand(String nodeId, int shardId) {
        return ("RELEASE " + nodeId + " " + shardId).getBytes(StandardCharsets.UTF_8);
    }
}
