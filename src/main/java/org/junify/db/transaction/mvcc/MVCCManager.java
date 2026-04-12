package org.junify.db.transaction.mvcc;

import org.junify.db.core.record.RecordMetadata;
import org.junify.db.core.record.UnifiedRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * MVCC Manager — provides snapshot isolation via versioned records.
 *
 * Each write creates a new version with a transaction-scoped timestamp.
 * Readers see the latest version committed before their transaction started.
 * Write-write conflicts are detected at commit time.
 */
public final class MVCCManager {

    private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    private final ConcurrentMap<String, VersionChain> versionStore = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, WriteBuffer> txWrites = new ConcurrentHashMap<>();

    /**
     * Allocate a monotonically increasing transaction timestamp.
     */
    public long assignTimestamp() {
        return clock.incrementAndGet();
    }

    /**
     * Read the version visible to a transaction with the given readTimestamp.
     * Returns null if no version is visible.
     */
    public UnifiedRecord read(String key, long readTimestamp, Function<String, ? extends UnifiedRecord> factory) {
        var chain = versionStore.get(key);
        if (chain == null) return null;

        // Find the latest version committed before readTimestamp
        var node = chain.head;
        UnifiedRecord visible = null;
        while (node != null) {
            if (node.commitTs <= readTimestamp) {
                visible = node.record;
                break;
            }
            node = node.next;
        }
        return visible != null ? visible : null;
    }

    /**
     * Stage a write for a transaction. The write is not visible to other
     * transactions until commit.
     */
    public void stageWrite(String txId, String key, UnifiedRecord record) {
        txWrites.computeIfAbsent(txId, k -> new WriteBuffer()).writes.put(key, record);
    }

    /**
     * Stage a delete for a transaction.
     */
    public void stageDelete(String txId, String key) {
        txWrites.computeIfAbsent(txId, k -> new WriteBuffer()).deletes.add(key);
    }

    /**
     * Commit all staged writes for a transaction.
     * Returns false if a write-write conflict is detected.
     */
    public boolean commit(String txId, long commitTs) {
        var buffer = txWrites.remove(txId);
        if (buffer == null) return true;

        // Check for write-write conflicts and apply
        for (var entry : buffer.writes.entrySet()) {
            var key = entry.getKey();
            var record = entry.getValue();
            var chain = versionStore.get(key);
            if (chain != null && chain.head != null && chain.head.commitTs > commitTs) {
                // Conflict: another transaction wrote after us
                return false;
            }
            var metadata = record.metadata().nextVersion(txId);
            var versionedRecord = record.withMetadata(metadata);
            versionStore.put(key, new VersionChain(new VersionNode(versionedRecord, commitTs)));
        }

        // Apply deletes
        for (var key : buffer.deletes) {
            versionStore.remove(key);
        }
        return true;
    }

    /**
     * Rollback all staged writes for a transaction.
     */
    public void rollback(String txId) {
        txWrites.remove(txId);
    }

    /**
     * Garbage collect old versions that are no longer visible to any active transaction.
     */
    public int vacuum(long minActiveTimestamp) {
        int collected = 0;
        for (var entry : versionStore.entrySet()) {
            var chain = entry.getValue();
            var prev = chain.head;
            while (prev != null && prev.next != null && prev.next.commitTs < minActiveTimestamp) {
                prev = prev.next;
                collected++;
            }
            if (chain.head != prev) {
                entry.setValue(new VersionChain(prev));
            }
        }
        return collected;
    }

    /**
     * Current version count across all keys.
     */
    public int versionCount() {
        int count = 0;
        for (var chain : versionStore.values()) {
            var node = chain.head;
            while (node != null) {
                count++;
                node = node.next;
            }
        }
        return count;
    }

    public Map<String, Object> stats() {
        return Map.of(
            "keys", versionStore.size(),
            "versions", versionCount(),
            "activeTransactions", txWrites.size(),
            "currentTimestamp", clock.get()
        );
    }

    // === Internal structures ===
    private record VersionNode(UnifiedRecord record, long commitTs, VersionNode next) {
        VersionNode(UnifiedRecord record, long commitTs) {
            this(record, commitTs, null);
        }
    }

    private record VersionChain(VersionNode head) {}

    private static class WriteBuffer {
        final Map<String, UnifiedRecord> writes = new ConcurrentHashMap<>();
        final List<String> deletes = new ArrayList<>();
    }
}
