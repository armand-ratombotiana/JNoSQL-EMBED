package org.junify.db.benchmark;

import org.junify.db.JunifyDB;
import org.junify.db.config.JunifyDBConfig;
import org.junify.db.index.hnsw.HNSWIndex;
import org.junify.db.index.hnsw.VectorIndex;
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
 * Vector Search Benchmarks — HNSW Index Performance
 * 
 * Measures:
 * - Vector insertion throughput
 * - K-NN search latency (top-K)
 * - Cosine similarity vs Euclidean distance
 * - Memory footprint per vector
 * 
 * SPEC.md Targets:
 * - Sub-millisecond similarity search for 100K vectors
 * - p99 latency <5ms for top-10 search
 * - Insert throughput >10K vectors/sec
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgsPrepend = "--enable-preview")
@Timeout(time = 300)
public class VectorSearchBenchmark {

    @Param({"128", "256", "512", "1024"})
    public int dimensions;

    @Param({"1000", "10000"})
    public int vectorCount;

    private VectorIndex hnswIndex;
    private float[][] vectors;
    private float[] queryVector;
    private final Random random = new Random(42);

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        var tempDir = Files.createTempDirectory("junify-vector-bench-");
        hnswIndex = new HNSWIndex.Builder()
                .dimensions(dimensions)
                .maxConnections(16)
                .efConstruction(200)
                .storagePath(tempDir.resolve("hnsw-" + dimensions))
                .build();

        // Generate random vectors
        vectors = new float[vectorCount][dimensions];
        for (int i = 0; i < vectorCount; i++) {
            for (int d = 0; d < dimensions; d++) {
                vectors[i][d] = random.nextFloat();
            }
            hnswIndex.insert(String.valueOf(i), vectors[i]);
        }

        // Generate query vector
        queryVector = new float[dimensions];
        for (int d = 0; d < dimensions; d++) {
            queryVector[d] = random.nextFloat();
        }
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        if (hnswIndex != null) hnswIndex.close();
    }

    @Benchmark
    public java.util.List<VectorIndex.SearchResult> vectorSearchTop10() {
        return hnswIndex.search(queryVector, 10);
    }

    @Benchmark
    public java.util.List<VectorIndex.SearchResult> vectorSearchTop100() {
        return hnswIndex.search(queryVector, 100);
    }

    @Benchmark
    public float vectorDistanceCosine() {
        return hnswIndex.distance(queryVector, vectors[0], VectorIndex.SimilarityMetric.COSINE);
    }

    @Benchmark
    public float vectorDistanceEuclidean() {
        return hnswIndex.distance(queryVector, vectors[0], VectorIndex.SimilarityMetric.EUCLIDEAN);
    }

    @Benchmark
    public void vectorInsert() {
        var newVector = new float[dimensions];
        for (int d = 0; d < dimensions; d++) {
            newVector[d] = random.nextFloat();
        }
        hnswIndex.insert("new-" + System.currentTimeMillis(), newVector);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(VectorSearchBenchmark.class.getSimpleName())
                .result("target/benchmark-vector.json")
                .resultFormat(ResultFormatType.JSON)
                .build();
        new Runner(opt).run();
    }
}
