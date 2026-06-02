package io.kinetis.core.shard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Dynamically acquires shard ownership using <em>session-level</em> Postgres advisory locks.
 *
 * <p>A single dedicated connection is held for the lifetime of this node. Session advisory locks
 * are tied to that connection — when the node crashes the connection closes and all locks are
 * automatically released, with no external coordinator.
 *
 * <p>A background rebalancer periodically checks the {@code cluster_nodes} heartbeat table to
 * estimate active peers and shed/acquire shards to converge on a fair distribution.
 *
 * <p><b>Why session locks, not transaction locks:</b> {@code pg_try_advisory_xact_lock} is
 * released at transaction commit, but JdbcTemplate borrows and returns connections per query —
 * so the lock would drop on every statement. Session locks persist until
 * {@code pg_advisory_unlock} or the connection closes.
 */
public class PostgresAdvisoryShardOwnership implements ShardOwnershipProvider, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PostgresAdvisoryShardOwnership.class);

    private final DataSource dataSource;
    private final String nodeId;
    private final int totalShards;
    private final Duration heartbeatInterval;

    private Connection lockConn;
    private final CopyOnWriteArraySet<Integer> owned = new CopyOnWriteArraySet<>();

    public PostgresAdvisoryShardOwnership(DataSource dataSource, String nodeId,
                                           int totalShards, Duration heartbeatInterval) {
        this.dataSource        = dataSource;
        this.nodeId            = nodeId;
        this.totalShards       = totalShards;
        this.heartbeatInterval = heartbeatInterval;
    }

    public void start() throws SQLException {
        lockConn = dataSource.getConnection();
        lockConn.setAutoCommit(true);
        registerNode();
        claimInitialShards();
        log.info("PostgresAdvisoryShardOwnership started: node={} shards={}/{}", nodeId, owned, totalShards);
    }

    @Override public Set<Integer> ownedShards() { return Collections.unmodifiableSet(new HashSet<>(owned)); }
    @Override public int totalShards()           { return totalShards; }

    public void rebalance() {
        try {
            int active      = countActiveNodes();
            int target      = (int) Math.ceil((double) totalShards / Math.max(1, active));
            int current     = owned.size();
            if      (current < target) tryAcquireMore(target - current);
            else if (current > target) releaseExcess(current - target);
            heartbeat();
        } catch (SQLException e) {
            log.warn("rebalance failed", e);
        }
    }

    public void heartbeat() {
        try (Connection conn = dataSource.getConnection()) {
            Integer[] shardArr = ownedShards().stream().sorted().toArray(Integer[]::new);
            try (PreparedStatement ps = conn.prepareStatement("""
                    INSERT INTO cluster_nodes (node_id, last_heartbeat, owned_shard_ids)
                    VALUES (?, now(), ?)
                    ON CONFLICT (node_id) DO UPDATE
                      SET last_heartbeat = now(), owned_shard_ids = EXCLUDED.owned_shard_ids
                    """)) {
                ps.setString(1, nodeId);
                ps.setArray(2, conn.createArrayOf("int2", shardArr));
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            log.warn("heartbeat failed for node {}", nodeId, e);
        }
    }

    @Override
    public void close() {
        try {
            if (lockConn != null && !lockConn.isClosed()) {
                try (PreparedStatement ps = lockConn.prepareStatement("SELECT pg_advisory_unlock_all()")) {
                    ps.execute();
                }
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement ps = conn.prepareStatement("DELETE FROM cluster_nodes WHERE node_id = ?")) {
                    ps.setString(1, nodeId);
                    ps.executeUpdate();
                }
                lockConn.close();
            }
        } catch (SQLException e) {
            log.warn("error closing advisory lock connection", e);
        }
        owned.clear();
    }

    private void registerNode() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("""
                     INSERT INTO cluster_nodes (node_id, last_heartbeat, owned_shard_ids)
                     VALUES (?, now(), '{}')
                     ON CONFLICT (node_id) DO UPDATE SET last_heartbeat = now()
                     """)) {
            ps.setString(1, nodeId);
            ps.executeUpdate();
        }
    }

    private void claimInitialShards() throws SQLException {
        for (int shard = 0; shard < totalShards; shard++) {
            if (tryLock(shard)) owned.add(shard);
        }
    }

    private void tryAcquireMore(int n) throws SQLException {
        List<Integer> candidates = allShards();
        Collections.shuffle(candidates);
        int acquired = 0;
        for (int shard : candidates) {
            if (!owned.contains(shard) && tryLock(shard)) {
                owned.add(shard);
                log.debug("acquired shard {} (rebalance)", shard);
                if (++acquired >= n) break;
            }
        }
    }

    private void releaseExcess(int n) throws SQLException {
        List<Integer> mine = new ArrayList<>(owned);
        Collections.shuffle(mine);
        int released = 0;
        for (int shard : mine) {
            if (unlock(shard)) {
                owned.remove(shard);
                log.debug("released shard {} (rebalance)", shard);
                if (++released >= n) break;
            }
        }
    }

    private int countActiveNodes() throws SQLException {
        long thresholdSeconds = heartbeatInterval.toSeconds() * 3;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT count(*) FROM cluster_nodes WHERE last_heartbeat > now() - make_interval(secs => ?)")) {
            ps.setLong(1, thresholdSeconds);
            try (ResultSet rs = ps.executeQuery()) { rs.next(); return Math.max(1, rs.getInt(1)); }
        }
    }

    private boolean tryLock(int shard) throws SQLException {
        try (PreparedStatement ps = lockConn.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
            ps.setLong(1, shard);
            try (ResultSet rs = ps.executeQuery()) { rs.next(); return rs.getBoolean(1); }
        }
    }

    private boolean unlock(int shard) throws SQLException {
        try (PreparedStatement ps = lockConn.prepareStatement("SELECT pg_advisory_unlock(?)")) {
            ps.setLong(1, shard);
            try (ResultSet rs = ps.executeQuery()) { rs.next(); return rs.getBoolean(1); }
        }
    }

    private List<Integer> allShards() {
        List<Integer> list = new ArrayList<>(totalShards);
        for (int i = 0; i < totalShards; i++) list.add(i);
        return list;
    }
}
