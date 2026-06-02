package io.kinetis.api.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/**
 * Tunables for the scheduler loops, bound from {@code application.yml} under the
 * {@code scheduler} prefix.
 *
 * <p>The lease/heartbeat relationship is the key safety knob: heartbeats must fire
 * several times within {@code leaseTtl} so a healthy long-running job never loses its lease,
 * while {@code leaseTtl} stays short enough that crash recovery is timely.
 */
@ConfigurationProperties(prefix = "scheduler")
public class SchedulerProperties {

    /** Stable identity for this node; surfaces as lease_owner. Defaults to hostname if blank. */
    private String nodeId = "";

    /** Max runs claimed per scheduler tick. */
    private int batchSize = 100;

    /** How long a lease is held before the reaper may reclaim it. */
    private Duration leaseTtl = Duration.ofSeconds(30);

    /** How often a running worker extends its lease. Must be well under leaseTtl. */
    private Duration heartbeatInterval = Duration.ofSeconds(10);

    /** Scheduler poll cadence. */
    private Duration pollInterval = Duration.ofMillis(500);

    /** Reaper scan cadence. */
    private Duration reaperInterval = Duration.ofSeconds(5);

    /** Max expired leases reclaimed per reaper tick. */
    private int reaperBatchSize = 100;

    /** How often the cron scheduler checks for due recurring jobs. */
    private Duration cronInterval = Duration.ofSeconds(1);

    // Sharding
    /** Total shards in the cluster. Must be identical on every node. */
    private int totalShards = 16;

    /**
     * Shards this node owns. Format: {@code "0-7"} (range), {@code "4"} (single),
     * {@code "all"} or {@code ""} (all shards — default for single-node).
     */
    private String ownedShards = "all";

    // Queue / rate limiting
    /** Max LEASED+RUNNING runs before the scheduler backs off. 0 = unlimited. */
    private int maxConcurrentRuns = 1000;

    /** How often the token-bucket refill task runs. */
    private Duration rateLimitRefillInterval = Duration.ofSeconds(1);

    // gRPC worker
    /** Port the worker gRPC server listens on when app.role=worker. */
    private int workerGrpcPort = 9090;

    /** Stable id for this worker process. */
    private String workerGrpcId = "";

    /** Host of the scheduler's gRPC endpoint (for worker self-registration). */
    private String schedulerGrpcHost = "";

    /** Port of the scheduler's gRPC endpoint. */
    private int schedulerGrpcPort = 9091;

    // --- getters / setters ---

    public String getNodeId()                            { return nodeId; }
    public void   setNodeId(String v)                    { nodeId = v; }
    public int    getBatchSize()                          { return batchSize; }
    public void   setBatchSize(int v)                    { batchSize = v; }
    public Duration getLeaseTtl()                        { return leaseTtl; }
    public void   setLeaseTtl(Duration v)                { leaseTtl = v; }
    public Duration getHeartbeatInterval()               { return heartbeatInterval; }
    public void   setHeartbeatInterval(Duration v)       { heartbeatInterval = v; }
    public Duration getPollInterval()                    { return pollInterval; }
    public void   setPollInterval(Duration v)            { pollInterval = v; }
    public Duration getReaperInterval()                  { return reaperInterval; }
    public void   setReaperInterval(Duration v)          { reaperInterval = v; }
    public int    getReaperBatchSize()                    { return reaperBatchSize; }
    public void   setReaperBatchSize(int v)              { reaperBatchSize = v; }
    public Duration getCronInterval()                    { return cronInterval; }
    public void   setCronInterval(Duration v)            { cronInterval = v; }
    public int    getTotalShards()                        { return totalShards; }
    public void   setTotalShards(int v)                  { totalShards = v; }
    public String getOwnedShards()                        { return ownedShards; }
    public void   setOwnedShards(String v)               { ownedShards = v; }
    public int    getMaxConcurrentRuns()                  { return maxConcurrentRuns; }
    public void   setMaxConcurrentRuns(int v)            { maxConcurrentRuns = v; }
    public Duration getRateLimitRefillInterval()         { return rateLimitRefillInterval; }
    public void   setRateLimitRefillInterval(Duration v) { rateLimitRefillInterval = v; }
    public int    getWorkerGrpcPort()                     { return workerGrpcPort; }
    public void   setWorkerGrpcPort(int v)               { workerGrpcPort = v; }
    public String getWorkerGrpcId()                       { return workerGrpcId; }
    public void   setWorkerGrpcId(String v)              { workerGrpcId = v; }
    public String getSchedulerGrpcHost()                  { return schedulerGrpcHost; }
    public void   setSchedulerGrpcHost(String v)         { schedulerGrpcHost = v; }
    public int    getSchedulerGrpcPort()                  { return schedulerGrpcPort; }
    public void   setSchedulerGrpcPort(int v)            { schedulerGrpcPort = v; }
}
