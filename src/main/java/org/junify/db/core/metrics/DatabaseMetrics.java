package org.junify.db.core.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class DatabaseMetrics {

    private final AtomicLong inserts = new AtomicLong();
    private final AtomicLong updates = new AtomicLong();
    private final AtomicLong deletes = new AtomicLong();
    private final AtomicLong reads = new AtomicLong();
    private final AtomicLong queries = new AtomicLong();
    private final AtomicLong transactions = new AtomicLong();
    private final AtomicLong transactionCommits = new AtomicLong();
    private final AtomicLong transactionRollbacks = new AtomicLong();
    private final Map<String, AtomicLong> collectionSizes = new ConcurrentHashMap<>();
    private volatile long startTime = System.currentTimeMillis();

    public void recordInsert() {
        inserts.incrementAndGet();
    }

    public void recordUpdate() {
        updates.incrementAndGet();
    }

    public void recordDelete() {
        deletes.incrementAndGet();
    }

    public void recordRead() {
        reads.incrementAndGet();
    }

    public void recordQuery() {
        queries.incrementAndGet();
    }

    public void recordTransaction() {
        transactions.incrementAndGet();
    }

    public void recordTransactionCommit() {
        transactionCommits.incrementAndGet();
    }

    public void recordTransactionRollback() {
        transactionRollbacks.incrementAndGet();
    }

    public void updateCollectionSize(String collection, long size) {
        collectionSizes.computeIfAbsent(collection, k -> new AtomicLong()).set(size);
    }

    public Map<String, Object> snapshot() {
        long uptimeMs = System.currentTimeMillis() - startTime;
        long totalOps = inserts.get() + updates.get() + deletes.get() + reads.get() + queries.get();
        var result = new java.util.LinkedHashMap<String, Object>();
        result.put("uptimeMs", uptimeMs);
        result.put("totalOperations", totalOps);
        result.put("opsPerSecond", uptimeMs > 0 ? (totalOps * 1000.0 / uptimeMs) : 0);
        result.put("inserts", inserts.get());
        result.put("updates", updates.get());
        result.put("deletes", deletes.get());
        result.put("reads", reads.get());
        result.put("queries", queries.get());
        result.put("transactions", transactions.get());
        result.put("transactionCommits", transactionCommits.get());
        result.put("transactionRollbacks", transactionRollbacks.get());
        result.put("collections", Map.copyOf(collectionSizes.entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()))));
        return result;
    }

    public void reset() {
        inserts.set(0);
        updates.set(0);
        deletes.set(0);
        reads.set(0);
        queries.set(0);
        transactions.set(0);
        transactionCommits.set(0);
        transactionRollbacks.set(0);
        collectionSizes.clear();
        startTime = System.currentTimeMillis();
    }
}
