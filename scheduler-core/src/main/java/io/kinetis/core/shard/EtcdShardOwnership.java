package io.kinetis.core.shard;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.support.CloseableClient;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

/**
 * Shard ownership via etcd TTL leases.
 *
 * <p>On startup creates an etcd lease ({@code ttlSeconds = 3 × heartbeatInterval}) and tries
 * to acquire {@code /kinetis/shards/{id}} for each shard. The lease keepalive keeps locks alive
 * while the node is running; a crash lets the lease expire and releases all locks automatically.
 *
 * <p>Trade-offs vs Postgres advisory locks: linearisable CAS, works across datacenters,
 * watch API for instant notification — but requires etcd in the stack.
 */
public class EtcdShardOwnership implements ShardOwnershipProvider, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(EtcdShardOwnership.class);
    private static final String SHARD_KEY_PREFIX = "/kinetis/shards/";

    private final Client client;
    private final String nodeId;
    private final int totalShards;
    private final long leaseTtlSeconds;

    private long leaseId = 0;
    private CloseableClient keepAliveClient;
    private final CopyOnWriteArraySet<Integer> owned = new CopyOnWriteArraySet<>();

    public EtcdShardOwnership(Client client, String nodeId, int totalShards, long leaseTtlSeconds) {
        this.client          = client;
        this.nodeId          = nodeId;
        this.totalShards     = totalShards;
        this.leaseTtlSeconds = leaseTtlSeconds;
    }

    public void start() throws Exception {
        LeaseGrantResponse lease = client.getLeaseClient().grant(leaseTtlSeconds).get(5, TimeUnit.SECONDS);
        leaseId = lease.getID();
        keepAliveClient = client.getLeaseClient().keepAlive(leaseId,
                new StreamObserver<LeaseKeepAliveResponse>() {
                    @Override public void onNext(LeaseKeepAliveResponse r) {}
                    @Override public void onError(Throwable t) { log.warn("etcd keepAlive error for lease {}", leaseId, t); }
                    @Override public void onCompleted() {}
                });
        claimAvailableShards();
        log.info("EtcdShardOwnership started: node={} leaseId={} owned={}", nodeId, leaseId, owned);
    }

    public void rebalance() {
        try {
            int target = Math.max(1, (int) Math.ceil((double) totalShards / estimatedNodeCount()));
            if (owned.size() < target) {
                claimAvailableShards();
            } else if (owned.size() > target) {
                List<Integer> mine = new ArrayList<>(owned);
                Collections.shuffle(mine);
                mine.subList(0, owned.size() - target).forEach(this::releaseShard);
            }
        } catch (Exception e) {
            log.warn("rebalance failed", e);
        }
    }

    @Override public Set<Integer> ownedShards() { return Collections.unmodifiableSet(new HashSet<>(owned)); }
    @Override public int totalShards()           { return totalShards; }

    @Override
    public void close() {
        try {
            if (keepAliveClient != null) keepAliveClient.close();
            if (leaseId != 0) client.getLeaseClient().revoke(leaseId).get(3, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("error closing etcd lease", e);
        }
        owned.clear();
    }

    private void claimAvailableShards() {
        for (int shard = 0; shard < totalShards; shard++) {
            if (owned.contains(shard)) continue;
            try {
                client.getLockClient().lock(shardKey(shard), leaseId).get(1, TimeUnit.SECONDS);
                owned.add(shard);
                log.debug("claimed shard {} via etcd", shard);
            } catch (Exception e) {
                // held by another node
            }
        }
    }

    private void releaseShard(int shard) {
        try {
            client.getLockClient().unlock(shardKey(shard)).get(2, TimeUnit.SECONDS);
            owned.remove(shard);
            log.debug("released shard {} (rebalance)", shard);
        } catch (Exception e) {
            log.warn("failed to release shard {}", shard, e);
        }
    }

    private int estimatedNodeCount() { return 1; }

    private static ByteSequence shardKey(int shardId) {
        return ByteSequence.from((SHARD_KEY_PREFIX + shardId).getBytes(StandardCharsets.UTF_8));
    }
}
