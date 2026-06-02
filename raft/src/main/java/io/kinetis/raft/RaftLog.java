package io.kinetis.raft;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The Raft replicated log. Thread-confined — only {@link RaftNode}'s single-threaded executor
 * may call these methods; callers must not hold a reference across await points.
 *
 * <p>Snapshot support: entries before {@link #snapshotIndex()} are discarded; their effect is
 * captured in the state-machine snapshot. The log retains entries from {@code snapshotIndex + 1}.
 */
public class RaftLog {

    private final List<LogEntry> entries = new ArrayList<>();
    private long snapshotIndex = 0;
    private long snapshotTerm  = 0;

    /** Append a new entry and return its assigned log index. */
    public long append(long term, byte[] command) {
        long index = lastIndex() + 1;
        entries.add(new LogEntry(index, term, command));
        return index;
    }

    /** Entry at {@code index}, or null if compacted or beyond the end. */
    public LogEntry get(long index) {
        if (index <= snapshotIndex) return null;
        int pos = (int) (index - snapshotIndex - 1);
        return (pos < 0 || pos >= entries.size()) ? null : entries.get(pos);
    }

    /**
     * Truncate from {@code fromIndex} onward and append {@code newEntries}.
     * Used during AppendEntries processing when the leader's log conflicts with ours.
     */
    public void replaceFrom(long fromIndex, List<LogEntry> newEntries) {
        if (fromIndex <= snapshotIndex) {
            long adjustedFrom = snapshotIndex + 1;
            int skip = (int) (adjustedFrom - fromIndex);
            newEntries = newEntries.subList(Math.min(skip, newEntries.size()), newEntries.size());
            fromIndex = adjustedFrom;
        }
        int pos = (int) (fromIndex - snapshotIndex - 1);
        if (pos >= 0 && pos <= entries.size())
            entries.subList(pos, entries.size()).clear();
        entries.addAll(newEntries);
    }

    /** Log index of the last entry (0 if empty / fully compacted). */
    public long lastIndex() {
        return entries.isEmpty() ? snapshotIndex : entries.get(entries.size() - 1).index();
    }

    /** Term of the last entry (snapshotTerm if empty). */
    public long lastTerm() {
        return entries.isEmpty() ? snapshotTerm : entries.get(entries.size() - 1).term();
    }

    /** Entries from {@code fromIndex} inclusive to the end. */
    public List<LogEntry> from(long fromIndex) {
        if (fromIndex > lastIndex() + 1) return Collections.emptyList();
        int pos = (int) Math.max(0, fromIndex - snapshotIndex - 1);
        return Collections.unmodifiableList(entries.subList(pos, entries.size()));
    }

    /** Compact everything up to and including {@code index}. */
    public void snapshot(long index, long term) {
        int pos = (int) (index - snapshotIndex - 1);
        if (pos >= 0 && pos < entries.size())
            entries.subList(0, pos + 1).clear();
        snapshotIndex = index;
        snapshotTerm  = term;
    }

    public long snapshotIndex() { return snapshotIndex; }
    public long snapshotTerm()  { return snapshotTerm; }

    /** Term at {@code index}, or snapshotTerm if compacted, or -1 if unknown. */
    public long termAt(long index) {
        if (index == snapshotIndex) return snapshotTerm;
        LogEntry e = get(index);
        return e == null ? -1 : e.term();
    }
}
