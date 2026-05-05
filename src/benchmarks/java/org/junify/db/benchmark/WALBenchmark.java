package org.junify.db.benchmark;

import org.junify.db.JunifyDB;
import org.junify.db.config.JunifyDBConfig;
import org.junify.db.nosql.document.Document;
import org.junify.db.nosql.document.DocumentCollection;
import org.junify.db.storage.spi.StorageEngine;
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
 * WAL (Write-Ahead Log) Benchmarks — Durability Overhead Measurement
 * 
 * Measures:
 * - WAL append throughput
 * - Flush latency (sync vs async)
 * - Checkpoint overhead
 * - Recovery time
 * 
 * SPEC.md Targets:
 * - WAL append >100K ops/sec
 * - Async flush latency <1ms
 * - Sync flush latency <5ms
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsPrepend = "--enable-preview")
@Timeout(time = 300)
public class WALBenchmark {

    @Param({"sync", "async"})
    public String flushMode;

    private JunifyDB db;
    private DocumentCollection collection;
    private final Random random = new Random(42);

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        var tempDir = Files.createTempDirectory("junify-wal-bench-");
        db = JunifyDB.embed()
                .storageEngine(JunifyDBConfig.StorageEngineType.FILE)
                .persistTo(tempDir.toString())
                .autoFlush(flushMode.equals("sync"))
                .flushIntervalMs(flushMode.equals("async") ? 100 : 0)
                .build();
        collection = db.documentCollection("wal-bench");
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        if (db != null) db.close();
    }

    @Benchmark
    public Document walAppend() {
        return collection.insert(Document.of("wal", flushMode)
                .add("seq", random.nextLong())
                .add("ts", System.currentTimeMillis()));
    }

    @Benchmark
    public void walFlush() {
        db.flush();
    }

    @Benchmark
    public java.util.Map<String, Object> walStats() {
        return db.metrics().all();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(WALBenchmark.class.getSimpleName())
                .result("target/benchmark-wal.json")
                .resultFormat(ResultFormatType.JSON)
                .build();
        new Runner(opt).run();
    }
}
