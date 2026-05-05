package org.junify.db.benchmark;

import org.junify.db.JunifyDB;
import org.junify.db.config.JunifyDBConfig;
import org.junify.db.nosql.document.Document;
import org.junify.db.nosql.document.DocumentCollection;
import org.junify.db.nosql.kv.KeyValueBucket;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Storage Engine Microbenchmarks — Per-Engine Performance Comparison
 * 
 * Benchmarks all 5 storage engines: IN_MEMORY, FILE, B_TREE, LSM_TREE, H2
 * 
 * SPEC.md Targets:
 * - In-Memory: >1M ops/sec
 * - File (async): >100K ops/sec
 * - H2 SQL: >50K queries/sec
 * - p50 latency <1ms (KV), <5ms (indexed), <50ms (hybrid/vector)
 * - Startup <200ms
 * - Heap <50MB idle
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsPrepend = "--enable-preview")
@Timeout(time = 300)
public class StorageEngineBenchmark {

    @Param({"IN_MEMORY", "FILE", "B_TREE", "LSM_TREE", "H2"})
    public String engineType;

    private JunifyDB db;
    private DocumentCollection collection;
    private KeyValueBucket kvBucket;
    private Path tempDir;
    private final Random random = new Random(42);

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        tempDir = Files.createTempDirectory("junify-benchmark-");
        var config = JunifyDBConfig.builder()
                .storageEngine(JunifyDBConfig.StorageEngineType.valueOf(engineType))
                .persistTo(tempDir.toString())
                .autoFlush(engineType.equals("FILE"))
                .buildConfig();
        db = JunifyDB.create(config);
        collection = db.documentCollection("bench");
        kvBucket = db.keyValueBucket("bench-kv");
        
        // Pre-populate with 10K documents for realistic benchmarks
        for (int i = 0; i < 10_000; i++) {
            collection.insert(Document.of("id", i)
                    .add("name", "user-" + i)
                    .add("email", "user" + i + "@example.com")
                    .add("score", random.nextDouble() * 100)
                    .add("age", 18 + random.nextInt(50)));
        }
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        if (db != null) db.close();
        if (tempDir != null) {
            tempDir.toFile().deleteOnExit();
        }
    }

    // === KEY-VALUE BENCHMARKS ===

    @Benchmark
    public String kvGet() {
        return kvBucket.get("key-" + (random.nextInt(1000)));
    }

    @Benchmark
    public void kvPut() {
        kvBucket.put("key-bench-" + random.nextInt(100), "value-" + random.nextInt(1000));
    }

    @Benchmark
    public void kvPutWithTTL() {
        kvBucket.put("key-ttl-" + random.nextInt(100), "value", 3600);
    }

    @Benchmark
    public long kvIncrement() {
        return kvBucket.increment("counter-" + engineType);
    }

    @Benchmark
    public void kvDelete() {
        kvBucket.delete("key-temp-" + random.nextInt(100));
    }

    // === DOCUMENT BENCHMARKS ===

    @Benchmark
    public Document docInsert() {
        return collection.insert(Document.of("name", "bench-" + random.nextInt(1000))
                .add("value", random.nextDouble())
                .add("timestamp", System.currentTimeMillis()));
    }

    @Benchmark
    public Document docGetById() {
        return collection.findById(String.valueOf(random.nextInt(10_000)));
    }

    @Benchmark
    public List<Document> docFindAll() {
        return collection.findAll();
    }

    @Benchmark
    public List<Document> docFindByExactMatch() {
        return collection.find(org.junify.db.nosql.document.Query.eq("name", "user-500"));
    }

    @Benchmark
    public List<Document> docFindByRange() {
        return collection.find(org.junify.db.nosql.document.Query.gt("score", 50.0).limit(100));
    }

    @Benchmark
    public List<Document> docFindByCompoundQuery() {
        return collection.find(
            org.junify.db.nosql.document.Query.builder()
                .add("age", org.junify.db.document.QueryCondition.GREATER_THAN, 30)
                .add("score", org.junify.db.document.QueryCondition.LESS_THAN, 75.0)
                .build()
                .limit(50)
        );
    }

    @Benchmark
    public long docCount() {
        return collection.count();
    }

    @Benchmark
    public Document docUpdate() {
        var id = String.valueOf(random.nextInt(1000));
        var doc = collection.findById(id);
        if (doc != null) {
            doc.add("updated", true);
            doc.add("updateTime", System.currentTimeMillis());
            return collection.update(doc);
        }
        return null;
    }

    @Benchmark
    public void docDelete() {
        var id = String.valueOf(random.nextInt(100));
        collection.deleteById(id);
    }

    // === BATCH OPERATIONS ===

    @Benchmark
    public List<Document> docBatchInsert10() {
        var docs = new java.util.ArrayList<Document>();
        for (int i = 0; i < 10; i++) {
            docs.add(Document.of("batch", engineType).add("i", i));
        }
        return collection.insertAll(docs);
    }

    @Benchmark
    public void kvBatchPut10() {
        var entries = new java.util.HashMap<String, String>();
        for (int i = 0; i < 10; i++) {
            entries.put("batch-" + i, "value-" + i);
        }
        kvBucket.putAll(entries);
    }

    // === METRICS ===

    @Benchmark
    public java.util.Map<String, Object> getMetrics() {
        return db.metrics().all();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(StorageEngineBenchmark.class.getSimpleName())
                .result("target/benchmark-storage-engine.json")
                .resultFormat(ResultFormatType.JSON)
                .build();
        new Runner(opt).run();
    }
}
