package io.kinetis.raft;

import io.kinetis.raft.rpc.InMemoryRaftRpc;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class RaftElectionTest {

    private static final long MIN = 150, MAX = 300, HB = 50, SETTLE = 800;
    private List<RaftNode> nodes;

    private List<RaftNode> cluster(int size) {
        InMemoryRaftRpc rpc = new InMemoryRaftRpc();
        List<String> ids = IntStream.range(0, size).mapToObj(i -> "node-" + i).toList();
        nodes = ids.stream().map(id -> {
            Set<String> peers = ids.stream().filter(p -> !p.equals(id)).collect(Collectors.toSet());
            return new RaftNode(id, peers, rpc, new NoOpSM(), MIN, MAX, HB);
        }).toList();
        nodes.forEach(rpc::register);
        nodes.forEach(RaftNode::start);
        return nodes;
    }

    @AfterEach void stop() { if (nodes != null) nodes.forEach(RaftNode::stop); }

    @Test
    void threeNodeClusterElectsExactlyOneLeader() throws InterruptedException {
        cluster(3);
        TimeUnit.MILLISECONDS.sleep(SETTLE);
        List<RaftNode> leaders = nodes.stream().filter(RaftNode::isLeader).toList();
        assertThat(leaders).hasSize(1);
        String leaderId = leaders.get(0).nodeId();
        nodes.forEach(n -> assertThat(n.currentLeader()).isEqualTo(leaderId));
    }

    @Test
    void allNodesInSameTerm() throws InterruptedException {
        cluster(3);
        TimeUnit.MILLISECONDS.sleep(SETTLE);
        long term = nodes.stream().filter(RaftNode::isLeader).findFirst()
                .map(RaftNode::currentTerm).orElse(-1L);
        assertThat(term).isPositive();
        nodes.forEach(n -> assertThat(n.currentTerm()).isEqualTo(term));
    }

    @Test
    void leadershipNotifiedViaCallback() throws InterruptedException {
        InMemoryRaftRpc rpc = new InMemoryRaftRpc();
        AtomicInteger fired = new AtomicInteger();
        List<String> ids = List.of("n0", "n1", "n2");
        nodes = ids.stream().map(id -> {
            Set<String> peers = ids.stream().filter(p -> !p.equals(id)).collect(Collectors.toSet());
            RaftNode n = new RaftNode(id, peers, rpc, new NoOpSM(), MIN, MAX, HB);
            n.onLeaderChange(leader -> { if (leader) fired.incrementAndGet(); });
            return n;
        }).toList();
        nodes.forEach(rpc::register);
        nodes.forEach(RaftNode::start);
        TimeUnit.MILLISECONDS.sleep(SETTLE);
        assertThat(fired.get()).isEqualTo(1);
    }

    @Test
    void proposalAppliedToAllNodes() throws InterruptedException {
        InMemoryRaftRpc rpc = new InMemoryRaftRpc();
        List<CountingSM> sms = List.of(new CountingSM(), new CountingSM(), new CountingSM());
        List<String> ids = List.of("n0", "n1", "n2");
        nodes = IntStream.range(0, 3).mapToObj(i -> {
            Set<String> peers = ids.stream().filter(p -> !p.equals(ids.get(i))).collect(Collectors.toSet());
            return new RaftNode(ids.get(i), peers, rpc, sms.get(i), MIN, MAX, HB);
        }).toList();
        nodes.forEach(rpc::register);
        nodes.forEach(RaftNode::start);
        TimeUnit.MILLISECONDS.sleep(SETTLE);

        nodes.stream().filter(RaftNode::isLeader).findFirst().orElseThrow()
                .propose(new byte[]{42});
        TimeUnit.MILLISECONDS.sleep(SETTLE / 2);

        sms.forEach(sm -> assertThat(sm.applied()).isGreaterThanOrEqualTo(1));
    }

    static class NoOpSM implements RaftStateMachine {
        @Override public void apply(long i, byte[] c) {}
        @Override public byte[] snapshot() { return new byte[0]; }
        @Override public void restoreSnapshot(long i, long t, byte[] s) {}
    }

    static class CountingSM implements RaftStateMachine {
        private final AtomicInteger count = new AtomicInteger();
        @Override public void apply(long i, byte[] c) { if (c.length > 0) count.incrementAndGet(); }
        @Override public byte[] snapshot() { return new byte[0]; }
        @Override public void restoreSnapshot(long i, long t, byte[] s) {}
        int applied() { return count.get(); }
    }
}
