package io.kinetis.core.shard;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Shard ownership fixed at startup from configuration. No coordination, no dynamic rebalancing.
 *
 * <p>Range spec: {@code "0-7"} = shards 0–7 inclusive; {@code "all"}, {@code ""}, or {@code "*"}
 * = all shards (default for single-node deployments); {@code "4"} = one shard only.
 */
public class StaticShardOwnership implements ShardOwnershipProvider {

    private final Set<Integer> ownedShards;
    private final int totalShards;

    public StaticShardOwnership(int totalShards, String rangeSpec) {
        this.totalShards = totalShards;
        this.ownedShards = parseRange(rangeSpec, totalShards);
    }

    @Override public Set<Integer> ownedShards() { return ownedShards; }
    @Override public int totalShards()           { return totalShards; }

    @Override
    public String toString() {
        return "StaticShardOwnership{shards=" + ownedShards + "/" + totalShards + "}";
    }

    static Set<Integer> parseRange(String spec, int totalShards) {
        if (spec == null || spec.isBlank() || "all".equalsIgnoreCase(spec) || "*".equals(spec))
            return Set.copyOf(IntStream.range(0, totalShards).boxed().collect(Collectors.toSet()));
        if (spec.contains("-")) {
            String[] parts = spec.split("-", 2);
            int from = Integer.parseInt(parts[0].trim());
            int to   = Integer.parseInt(parts[1].trim());
            if (from < 0 || to >= totalShards || from > to)
                throw new IllegalArgumentException("shard range " + spec + " invalid for totalShards=" + totalShards);
            return Set.copyOf(IntStream.rangeClosed(from, to).boxed().collect(Collectors.toSet()));
        }
        int single = Integer.parseInt(spec.trim());
        if (single < 0 || single >= totalShards)
            throw new IllegalArgumentException("shard " + single + " out of range for totalShards=" + totalShards);
        return Set.of(single);
    }
}
