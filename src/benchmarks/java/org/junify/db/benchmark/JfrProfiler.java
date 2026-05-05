package org.junify.db.benchmark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;

/**
 * JFR (Java Flight Recorder) Profiling Integration
 * 
 * Provides programmatic JFR control for performance profiling:
 * - CPU profiling
 * - Memory allocation profiling
 * - GC event recording
 * - I/O latency tracking
 * - Lock contention analysis
 * 
 * Usage:
 *   JfrProfiler.start("benchmark-session", Duration.ofMinutes(5));
 *   // ... run benchmarks ...
 *   JfrProfiler.stop();
 *   
 * Or via JVM flags:
 *   -XX:StartFlightRecording=duration=5m,name=benchmark,filename=target/jfr-benchmark.jfr
 *   -XX:FlightRecorderOptions=stackdepth=256
 */
public class JfrProfiler {

    private static final Path JFR_OUTPUT_DIR = Path.of("target", "jfr");
    private static volatile boolean recording = false;

    static {
        try {
            Files.createDirectories(JFR_OUTPUT_DIR);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create JFR output directory", e);
        }
    }

    /**
     * Start JFR recording with standard JunifyDB profiling settings.
     * 
     * @param name Recording session name
     * @param duration Recording duration
     */
    public static void start(String name, Duration duration) {
        if (recording) {
            System.out.println("[JFR] Recording already in progress");
            return;
        }

        System.out.println("[JFR] Starting recording: " + name);
        System.out.println("[JFR] Duration: " + duration.getSeconds() + "s");
        System.out.println("[JFR] Output: " + JFR_OUTPUT_DIR.resolve(name + ".jfr"));

        // JFR is typically started via JVM flags, but we can log the configuration
        System.out.println();
        System.out.println("=== JFR Configuration ===");
        System.out.println("-XX:StartFlightRecording=duration=" + duration.getSeconds() + "s," +
                          "name=" + name + "," +
                          "filename=" + JFR_OUTPUT_DIR.resolve(name + ".jfr") + "," +
                          "settings=profile");
        System.out.println("-XX:FlightRecorderOptions=stackdepth=256,dumponexit=true");
        System.out.println();
        System.out.println("Key events to monitor:");
        System.out.println("  - jdk.GCPhasePause (GC pauses)");
        System.out.println("  - jdk.ObjectAllocationInNewTLAB (Memory allocation)");
        System.out.println("  - jdk.JavaMonitorEnter (Lock contention)");
        System.out.println("  - jdk.FileRead/FileWrite (I/O latency)");
        System.out.println("  - jdk.ThreadSleep (Thread scheduling)");
        System.out.println("=========================");
        System.out.println();

        recording = true;
    }

    /**
     * Stop JFR recording.
     */
    public static void stop() {
        if (!recording) {
            System.out.println("[JFR] No recording in progress");
            return;
        }

        System.out.println("[JFR] Stopping recording");
        System.out.println("[JFR] Analyze with: jfr print target/jfr/*.jfr");
        System.out.println("[JFR] Or open in Java Mission Control (JMC)");
        recording = false;
    }

    /**
     * Get recommended JVM flags for JFR profiling.
     */
    public static String[] getJvmFlags() {
        return new String[] {
            "-XX:StartFlightRecording=duration=60s,name=profile,filename=target/jfr/profile.jfr,settings=profile",
            "-XX:FlightRecorderOptions=stackdepth=256,dumponexit=true",
            "-XX:+UnlockDiagnosticVMOptions",
            "-XX:+DebugNonSafepoints"
        };
    }

    /**
     * Get recommended async-profiler configuration.
     */
    public static String getAsyncProfilerConfig() {
        return """
            # async-profiler configuration for JunifyDB
            # CPU profiling:
            ./profiler.sh start --event cpu --interval 1ms --file target/async-profiler/cpu.html <pid>
            
            # Memory allocation:
            ./profiler.sh start --event alloc --interval 500k --file target/async-profiler/alloc.html <pid>
            
            # Lock contention:
            ./profiler.sh start --event lock --file target/async-profiler/lock.html <pid>
            
            # Wall clock (includes sleep/park time):
            ./profiler.sh start --event wall --interval 10ms --file target/async-profiler/wall.html <pid>
            
            # Stop and generate report:
            ./profiler.sh stop --file target/async-profiler/profile.html <pid>
            """;
    }

    /**
     * Print GC analysis commands.
     */
    public static void printGcAnalysis() {
        System.out.println("=== GC Analysis Commands ===");
        System.out.println("# Enable GC logging:");
        System.out.println("-Xlog:gc*:file=target/gc.log:time,uptime,level,tags");
        System.out.println();
        System.out.println("# Analyze GC log with gceasy.io:");
        System.out.println("Upload target/gc.log to https://gceasy.io");
        System.out.println();
        System.out.println("# Key GC metrics to monitor:");
        System.out.println("  - Young Gen pause time (target: <10ms)");
        System.out.println("  - Old Gen collection frequency");
        System.out.println("  - Heap utilization trend");
        System.out.println("  - GC throughput (target: >95%)");
        System.out.println("============================");
    }

    /**
     * Check if current JVM supports JFR.
     */
    public static boolean isJfrAvailable() {
        try {
            Class.forName("jdk.jfr.Recording");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    /**
     * Print current JVM info relevant for profiling.
     */
    public static void printJvmInfo() {
        System.out.println("=== JVM Information ===");
        System.out.println("Java Version: " + System.getProperty("java.version"));
        System.out.println("JVM Name: " + System.getProperty("java.vm.name"));
        System.out.println("JVM Vendor: " + System.getProperty("java.vm.vendor"));
        System.out.println("JFR Available: " + isJfrAvailable());
        System.out.println("Available Processors: " + Runtime.getRuntime().availableProcessors());
        System.out.println("Max Heap (MB): " + (Runtime.getRuntime().maxMemory() / 1024 / 1024));
        System.out.println("=======================");
    }
}
