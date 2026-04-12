package org.junify.db.benchmark;

import org.junify.db.JunifyDB;
import org.junify.db.document.Document;
import org.junify.db.document.DocumentCollection;
import org.junify.db.document.Query;
import org.junify.db.kv.KeyValueBucket;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class JunifyDBBenchmark {

    private JunifyDB db;
    private DocumentCollection collection;
    private KeyValueBucket bucket;

    @Setup
    public void setup() {
        db = JUNIFYDB.embed().build();
        collection = db.documentCollection("benchmark");
        bucket = db.keyValueBucket("benchmark-kv");
        for (int i = 0; i < 1000; i++) {
            collection.insert(Document.of("id", i).add("name", "user-" + i).add("score", Math.random() * 100));
        }
    }

    @TearDown
    public void tearDown() {
        db.close();
    }

    @Benchmark
    public Document documentInsert() {
        return collection.insert(Document.of("name", "bench").add("value", 42));
    }

    @Benchmark
    public Document documentFindById() {
        return collection.findById("0");
    }

    @Benchmark
    public List<Document> documentFindAll() {
        return collection.findAll();
    }

    @Benchmark
    public List<Document> documentFindByQuery() {
        return collection.find(Query.eq("name", "user-500"));
    }

    @Benchmark
    public List<Document> documentFindByRange() {
        return collection.find(Query.gt("score", 50).limit(10));
    }

    @Benchmark
    public String kvPut() {
        bucket.put("key", "value");
        return "value";
    }

    @Benchmark
    public String kvGet() {
        bucket.put("key", "value");
        return bucket.get("key");
    }

    @Benchmark
    public long kvIncrement() {
        return bucket.increment("counter");
    }

    @Benchmark
    public long collectionCount() {
        return collection.count();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(JunifyDBBenchmark.class.getSimpleName())
                .result("target/benchmark-results.json")
                .resultFormat(ResultFormatType.JSON)
                .build();
        new Runner(opt).run();
    }
}
