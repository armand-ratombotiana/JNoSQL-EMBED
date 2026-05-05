package org.junify.db.benchmark;

import org.junify.db.JunifyDB;
import org.junify.db.config.JunifyDBConfig;
import org.junify.db.nosql.document.Document;
import org.junify.db.nosql.document.DocumentCollection;
import org.junify.db.transaction.Transaction;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * MVCC Transaction Benchmarks — Snapshot Isolation Performance
 * 
 * Measures:
 * - Transaction start/commit overhead
 * - Read-write transaction throughput
 * - Write-write conflict detection cost
 * - Snapshot isolation latency
 * - Version chain GC impact
 * 
 * SPEC.md Targets:
 * - Transaction commit overhead <100μs
 * - Zero lock contention on read paths
 * - GC pause <10ms (young gen)
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsPrepend = "--enable-preview")
@Timeout(time = 300)
public class MVCCBenchmark {

    private JunifyDB db;
    private DocumentCollection collection;
    private final Random random = new Random(42);

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        var tempDir = Files.createTempDirectory("junify-mvcc-bench-");
        db = JunifyDB.embed()
                .storageEngine(JunifyDBConfig.StorageEngineType.IN_MEMORY)
                .persistTo(tempDir.toString())
                .build();
        collection = db.documentCollection("mvcc-bench");
        
        // Pre-populate
        for (int i = 0; i < 1000; i++) {
            collection.insert(Document.of("id", i).add("value", 100));
        }
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        if (db != null) db.close();
    }

    @Benchmark
    public Transaction transactionStartCommit() {
        var tx = db.beginTransaction();
        tx.begin();
        var doc = collection.insert(Document.of("tx", "bench").add("value", random.nextInt(100)));
        tx.commit();
        return tx;
    }

    @Benchmark
    public Document transactionReadIsolation() {
        var tx = db.beginTransaction();
        tx.begin();
        var doc = collection.findById(String.valueOf(random.nextInt(1000)));
        tx.commit();
        return doc;
    }

    @Benchmark
    public Document transactionWriteWithCommit() {
        var tx = db.beginTransaction();
        tx.begin();
        var id = String.valueOf(random.nextInt(100));
        var doc = collection.findById(id);
        if (doc != null) {
            doc.add("value", random.nextInt(100));
            collection.update(doc);
        }
        tx.commit();
        return doc;
    }

    @Benchmark
    public void transactionRollback() {
        var tx = db.beginTransaction();
        tx.begin();
        collection.insert(Document.of("rollback", "bench"));
        tx.rollback();
    }

    @Benchmark
    public long mvccTimestampAllocation() {
        return db.mvcc().assignTimestamp();
    }

    @Benchmark
    public java.util.Map<String, Object> mvccStats() {
        return db.mvcc().stats();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MVCCBenchmark.class.getSimpleName())
                .result("target/benchmark-mvcc.json")
                .resultFormat(ResultFormatType.JSON)
                .build();
        new Runner(opt).run();
    }
}
