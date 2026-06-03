package io.kinetis.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * File-based Raft persistence. Writes an atomic rename (write-to-temp → rename) so the file
 * is never partially updated — a crash mid-write leaves the old file intact.
 *
 * <p>Format: two lines — {@code currentTerm} (decimal) and {@code votedFor} (node ID or
 * the literal {@code null}).
 *
 * <pre>
 *   // Example: term 4, voted for node-2
 *   4
 *   node-2
 * </pre>
 *
 * <p>The file is stored at {@code <dataDir>/raft-<nodeId>.state}.
 */
public class FileRaftPersistence implements RaftPersistence {

    private static final Logger log = LoggerFactory.getLogger(FileRaftPersistence.class);

    private final Path stateFile;
    private final Path tempFile;

    public FileRaftPersistence(Path dataDir, String nodeId) {
        this.stateFile = dataDir.resolve("raft-" + nodeId + ".state");
        this.tempFile  = dataDir.resolve("raft-" + nodeId + ".state.tmp");
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new IllegalStateException("cannot create Raft data directory: " + dataDir, e);
        }
    }

    @Override
    public PersistedState load() {
        if (!Files.exists(stateFile)) return PersistedState.initial();
        try {
            String[] lines = Files.readString(stateFile, StandardCharsets.UTF_8).trim().split("\\R", 2);
            long   term     = Long.parseLong(lines[0].trim());
            String votedFor = lines.length > 1 && !lines[1].trim().equals("null")
                    ? lines[1].trim() : null;
            log.debug("loaded Raft state: term={} votedFor={}", term, votedFor);
            return new PersistedState(term, votedFor);
        } catch (Exception e) {
            log.warn("corrupt Raft state file {}; resetting to initial", stateFile, e);
            return PersistedState.initial();
        }
    }

    @Override
    public void save(long currentTerm, String votedFor) {
        String content = currentTerm + "\n" + (votedFor == null ? "null" : votedFor) + "\n";
        try {
            // Atomic write: write to temp, then rename into place
            Files.writeString(tempFile, content, StandardCharsets.UTF_8);
            Files.move(tempFile, stateFile, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            // Persistence failure is a hard error — the node must not proceed without saving.
            throw new IllegalStateException(
                    "failed to persist Raft state (term=" + currentTerm + "): " + e.getMessage(), e);
        }
    }
}
