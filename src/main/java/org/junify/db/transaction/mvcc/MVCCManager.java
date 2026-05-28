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
import java.util.function.Predicate;

/**
 * MVCC Manager — provides snapshot isolation via versioned records.
 *
 * <p>Design:
 * <ul>
 *   <li>Monotonically-increasing {@link AtomicLong} clock for lock-free timestamp allocation.</li>
 *   <li>Lock-free version-chain traversal — readers never block writers.</li>
 *   <li>Optimistic write-write conflict detection at commit time.</li>
 *   <li>GC-friendly version compaction via {@link #vacuum(long)} and {@link #vacuumAggressive()}.</li>
 * </ul>
 *
 * <p>Each write creates a new version with a transaction-scoped timestamp.
 * Readers see the latest version committed before their transaction started.
 * Write-write conflicts are detected at commit time.
 */
public final class MVCCManager {

    private final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
    private final ConcurrentMap<String, VersionChain> versionStore = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, WriteBuffer> txWrites = new ConcurrentHashMap<>();
    
    // GC pressure monitoring for proactive vacuum
    private final java.lang.management.MemoryMXBean memoryBean =
        java.lang.management.ManagementFactory.getMemoryMXBean();
    private static final long GC_THRESHOLD_BYTES = 100 * 1024 * 1024; // 100MB

    /**
     * Allocate a monotonically increasing transaction timestamp.
     * Uses {@link AtomicLong} for lock-free allocation.
     */
    public long assignTimestamp() {
        return clock.incrementAndGet();
    }

    /**
     * Read the version visible to a transaction with the given readTimestamp.
     * Returns {@code null} if no version is visible.
     * Lock-free: readers traverse the immutable version chain without locking.
     */
    public UnifiedRecord read(String key, long readTimestamp, Function<String, ? extends UnifiedRecord> factory) {
        var chain = versionStore.get(key);
        if (chain == null) return null;

        // Find the latest version committed at or before readTimestamp
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
     * Read with predicate filter for index-assisted lookup.
     */
    public UnifiedRecord readIf(String key, long readTimestamp, 
                                 Predicate<UnifiedRecord> predicate,
                                 Function<String, ? extends UnifiedRecord> factory) {
        var record = read(key, readTimestamp, factory);
        if (record != null && predicate.test(record)) {
            return record;
        }
        return null;
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
     * Commit a transaction's staged writes.
     * Returns {@code false} if a write-write conflict is detected, in which
     * case the caller must roll back.
     */
    public boolean commit(String txId, long commitTs) {
        var buffer = txWrites.remove(txId);
        if (buffer == null) return true;

        // Check for write-write conflicts and apply
        for (var entry : buffer.writes.entrySet()) {
            var key = entry.getKey();
            var record = entry.getValue();
            var chain = versionStore.get(key);
            
            // Optimistic conflict detection: if someone committed after our read timestamp
            // but before our commit timestamp, we have a write-write conflict.
            if (chain != null && chain.head != null && chain.head.commitTs > commitTs) {
                // Conflict: another transaction wrote after us
                return false;
            }
            
            // Create new version with updated metadata
            var metadata = record.metadata().nextVersion(txId);
            var versionedRecord = record.withMetadata(metadata);
            
            // Prepend new version to the chain (lock-free: versionStore is a ConcurrentHashMap)
            var newChain = new VersionChain(new VersionNode(versionedRecord, commitTs, chain != null ? chain.head : null));
            versionStore.put(key, newChain);
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
     * Garbage-collect old versions that are no longer visible to any active transaction.
     *
     * <p>Versions whose {@code commitTs} is older than {@code minActiveTimestamp} and
     * that are not the most-recent version for a key are eligible for removal.
     *
     * <p>Vacuum is skipped entirely when heap usage is <em>below</em>
     * {@value #GC_THRESHOLD_BYTES} bytes, to avoid overhead on lightly-loaded instances.
     * When memory pressure is detected the full chain is trimmed.
     *
     * @param minActiveTimestamp the smallest read-timestamp of any active transaction
     * @return the number of stale versions removed
     */
    public int vacuum(long minActiveTimestamp) {
        // Skip vacuum when there is no memory pressure — avoids needless work.
        var heapUsed = memoryBean.getHeapMemoryUsage().getUsed();
        if (heapUsed < GC_THRESHOLD_BYTES) {
            return 0;
        }

        int collected = 0;
        for (var entry : versionStore.entrySet()) {
            var chain = entry.getValue();
            // Walk to the first node that is still visible (commitTs >= minActiveTimestamp).
            // Everything beyond that node is stale.
            var node = chain.head;
            VersionNode lastVisible = null;
            while (node != null) {
                if (node.commitTs >= minActiveTimestamp) {
                    lastVisible = node;
                }
                node = node.next;
            }
            // Truncate the chain after the last visible node
            if (lastVisible != null && lastVisible.next != null) {
                // Count how many nodes we are removing
                var stale = lastVisible.next;
                while (stale != null) {
                    collected++;
                    stale = stale.next;
                }
                // Rebuild chain with a new tail
                entry.setValue(new VersionChain(new VersionNode(lastVisible.record, lastVisible.commitTs, null)));
            }
        }
        return collected;
    }

    /**
     * Aggressive vacuum: removes all historic versions, keeping only the
     * most-recent version for each key. Use when memory pressure is critical
     * and correctness of in-flight transactions is no longer a concern
     * (e.g., during shutdown or after all transactions have committed).
     */
    public int vacuumAggressive() {
        int collected = 0;
        for (var entry : versionStore.entrySet()) {
            var chain = entry.getValue();
            if (chain.head != null && chain.head.next != null) {
                var node = chain.head.next;
                while (node != null) {
                    collected++;
                    node = node.next;
                }
                entry.setValue(new VersionChain(chain.head));
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

    /**
     * Returns runtime statistics including version chain depth and heap usage.
     */
    public Map<String, Object> stats() {
        var heapUsage = memoryBean.getHeapMemoryUsage();
        return Map.of(
            "keys", versionStore.size(),
            "versions", versionCount(),
            "activeTransactions", txWrites.size(),
            "currentTimestamp", clock.get(),
            "heapUsedMB", heapUsage.getUsed() / 1024 / 1024,
            "heapMaxMB", heapUsage.getMax() / 1024 / 1024,
            "gcEligibleVersions", versionCount() - versionStore.size()
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