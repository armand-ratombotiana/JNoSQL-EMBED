package org.junify.db.benchmark;

import org.junify.db.JunifyDB;
import org.junify.db.config.JunifyDBConfig;
import org.junify.db.config.JunifyDBConfig.StorageEngineType;
import org.junify.db.nosql.document.Document;
import org.junify.db.nosql.document.DocumentCollection;
import org.junify.db.nosql.kv.KeyValueBucket;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class BenchmarkRunner {

    public static void main(String[] args) throws Exception {
        var options = parseArgs(args);
        
        System.out.println("=".repeat(60));
        System.out.println("JunifyDB Benchmark Runner");
        System.out.println("=".repeat(60));
        
        var results = new BenchmarkResults();
        
        if (options.workload.contains("document") || options.workload.contains("all")) {
            runDocumentBenchmark(options, results);
        }
        
        if (options.workload.contains("kv") || options.workload.contains("all")) {
            runKeyValueBenchmark(options, results);
        }
        
        if (options.workload.contains("mixed") || options.workload.contains("all")) {
            runMixedBenchmark(options, results);
        }
        
        results.printSummary();
    }

    private static void runDocumentBenchmark(Options options, BenchmarkResults results) {
        System.out.println("\n--- Document Benchmark ---");
        
        var config = JunifyDB.embed()
            .storageEngine(options.engine)
            .autoFlush(false)
            .buildConfig();
        
        try (var db = JunifyDB.create(config)) {
            var collection = db.documentCollection("benchmark_docs");
            
            var writeTime = benchmarkWrites(options, collection);
            results.record("Document Write", options.ops, writeTime);
            
            var readTime = benchmarkReads(options, collection);
            results.record("Document Read", options.ops, readTime);
            
            collection.clear();
        }
    }

    private static void runKeyValueBenchmark(Options options, BenchmarkResults results) {
        System.out.println("\n--- Key-Value Benchmark ---");
        
        var config = JunifyDB.embed()
            .storageEngine(options.engine)
            .autoFlush(false)
            .buildConfig();
        
        try (var db = JunifyDB.create(config)) {
            var bucket = db.keyValueBucket("benchmark_kv");
            
            var writeTime = benchmarkKVWrites(options, bucket);
            results.record("KV Write", options.ops, writeTime);
            
            var readTime = benchmarkKVReads(options, bucket);
            results.record("KV Read", options.ops, readTime);
            
            bucket.clear();
        }
    }

    private static void runMixedBenchmark(Options options, BenchmarkResults results) {
        System.out.println("\n--- Mixed Benchmark ---");
        
        var config = JunifyDB.embed()
            .storageEngine(options.engine)
            .autoFlush(false)
            .buildConfig();
        
        try (var db = JunifyDB.create(config)) {
            var collection = db.documentCollection("mixed_docs");
            var bucket = db.keyValueBucket("mixed_kv");
            
            var executor = Executors.newFixedThreadPool(options.threads);
            var barrier = new CyclicBarrier(2);
            
            var start = System.nanoTime();
            
            for (int i = 0; i < options.threads; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        barrier.await();
                        for (int j = 0; j < options.ops / options.threads; j++) {
                            var doc = new Document();
                            doc.id("doc-" + threadId + "-" + j);
                            doc.add("data", UUID.randomUUID().toString());
                            collection.insert(doc);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
            
            executor.shutdown();
            executor.awaitTermination(60, TimeUnit.SECONDS);
            
            var elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            results.record("Mixed Write", options.ops, elapsed);
            results.record("Mixed Throughput", options.ops, 
                (options.ops * 1000L) / Math.max(elapsed, 1));
        }
    }

    private static long benchmarkWrites(Options options, DocumentCollection collection) {
        var docs = new ArrayList<Document>(options.ops);
        for (int i = 0; i < options.ops; i++) {
            var doc = new Document();
            doc.id("doc-" + i);
            doc.add("index", i);
            doc.add("data", "data-" + i);
            doc.add("timestamp", System.currentTimeMillis());
            docs.add(doc);
        }
        
        var start = System.nanoTime();
        for (var doc : docs) {
            collection.insert(doc);
        }
        
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }

    private static long benchmarkReads(Options options, DocumentCollection collection) {
        var start = System.nanoTime();
        for (int i = 0; i < options.ops; i++) {
            collection.findById("doc-" + i);
        }
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }

    private static long benchmarkKVWrites(Options options, KeyValueBucket bucket) {
        var start = System.nanoTime();
        for (int i = 0; i < options.ops; i++) {
            bucket.put("key-" + i, "value-" + i);
        }
        
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }

    private static long benchmarkKVReads(Options options, KeyValueBucket bucket) {
        var start = System.nanoTime();
        for (int i = 0; i < options.ops; i++) {
            bucket.get("key-" + i);
        }
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    }

    private static Options parseArgs(String[] args) {
        var options = new Options();
        
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--ops" -> options.ops = Integer.parseInt(args[++i]);
                case "--engine" -> options.engine = switch (args[++i].toUpperCase()) {
                    case "FILE" -> StorageEngineType.FILE;
                    case "LSM_TREE" -> StorageEngineType.LSM_TREE;
                    case "B_TREE" -> StorageEngineType.B_TREE;
                    case "H2" -> StorageEngineType.H2;
                    default -> StorageEngineType.IN_MEMORY;
                };
                case "--threads" -> options.threads = Integer.parseInt(args[++i]);
                case "--workload" -> options.workload = args[++i];
            }
        }
        
        return options;
    }

    static class Options {
        int ops = 10000;
        StorageEngineType engine = StorageEngineType.IN_MEMORY;
        int threads = 1;
        String workload = "all";
    }

    static class BenchmarkResults {
        private final Map<String, Result> results = new LinkedHashMap<>();

        void record(String name, int ops, long timeMs) {
            var throughput = (ops * 1000L) / Math.max(timeMs, 1);
            var latencyMs = (double) timeMs / ops * 1000;
            results.put(name, new Result(ops, timeMs, throughput, latencyMs));
        }

        void printSummary() {
            System.out.println("\n" + "=".repeat(60));
            System.out.println("BENCHMARK RESULTS");
            System.out.println("=".repeat(60));
            
            for (var entry : results.entrySet()) {
                var r = entry.getValue();
                System.out.printf("%-25s %,10d ops in %,6d ms (%,8d ops/s) lat: %.3f µs%n",
                    entry.getKey(), r.ops(), r.timeMs(), r.throughput(), r.latencyUs());
            }
        }

        record Result(int ops, long timeMs, long throughput, double latencyUs) {
            public int ops() { return ops; }
            public long timeMs() { return timeMs; }
            public long throughput() { return throughput; }
            public double latencyUs() { return latencyUs; }
        }
    }
}