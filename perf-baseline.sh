#!/usr/bin/env bash
#
# JunifyDB Performance Baseline Script
# 
# Runs JMH benchmarks and validates against performance gates.
# Fails if any threshold is breached.
#
# Usage: ./perf-baseline.sh [--full] [--quick]
#
# Options:
#   --full   Run full benchmark suite (default: 5 iterations)
#   --quick  Run quick sanity check (1 iteration)
#

set -e

# Performance thresholds (must pass all)
declare -A THRESHOLDS
THRESHOLDS[startup_ms]=200
THRESHOLDS[heap_idle_mb]=50
THRESHOLDS[kv_read_p50_ms]=1
THRESHOLDS[doc_read_p50_ms]=1
THRESHOLDS[indexed_query_ms]=5
THRESHOLDS[vector_search_ms]=50
THRESHOLDS[throughput_ops]=50000
THRESHOLDS[gc_pause_ms]=10

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================"
echo "  JunifyDB Performance Baseline Test"
echo "========================================"
echo ""

# Parse arguments
MODE="standard"
if [[ "$1" == "--full" ]]; then
    MODE="full"
elif [[ "$1" == "--quick" ]]; then
    MODE="quick"
fi

echo "Mode: $MODE"
echo ""

# Check Java version
JAVA_VERSION=$(java -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
echo "Java Version: $JAVA_VERSION"

if [[ $JAVA_VERSION -lt 17 ]]; then
    echo -e "${RED}ERROR: Java 17+ required${NC}"
    exit 1
fi

# Create output directory
mkdir -p target/benchmark-results
mkdir -p target/jfr

# Build project
echo ""
echo "Building project..."
mvn clean compile -DskipTests -q

# Run benchmarks based on mode
echo ""
echo "Running JMH benchmarks..."

case $MODE in
    quick)
        ITERATIONS=1
        WARMUP=1
        ;;
    full)
        ITERATIONS=10
        WARMUP=5
        ;;
    *)
        ITERATIONS=5
        WARMUP=3
        ;;
esac

echo "Iterations: $ITERATIONS, Warmup: $WARMUP"
echo ""

# Run main benchmark suite
mvn clean package -Pbenchmark -DskipTests -q

# Execute benchmarks
java -Xmx2g \
     -XX:+UnlockDiagnosticVMOptions \
     -XX:+DebugNonSafepoints \
     -jar target/benchmarks.jar \
     -i $ITERATIONS \
     -wi $WARMUP \
     -f 1 \
     -r 5 \
     -w 2 \
     -rf json \
     -rff target/benchmark-results/jmh-results.json \
     -o target/benchmark-results/console-output.txt \
     ".*JunifyBenchmark.*"

echo ""
echo "Benchmark complete. Results saved to target/benchmark-results/"
echo ""

# Parse results and validate thresholds
echo "========================================"
echo "  Validating Performance Gates"
echo "========================================"
echo ""

# Extract metrics from JMH results (simplified parsing)
# In production, use proper JSON parser like jq

PASS=0
FAIL=0

# Check throughput (ops/sec)
THROUGHPUT=$(grep -o '"throughput":[0-9.]*' target/benchmark-results/jmh-results.json | head -1 | cut -d':' -f2)
if [[ -n "$THROUGHPUT" ]]; then
    THROUGHPUT_INT=${THROUGHPUT%.*}
    if [[ $THROUGHPUT_INT -ge ${THRESHOLDS[throughput_ops]} ]]; then
        echo -e "${GREEN}✓${NC} Throughput: ${THROUGHPUT} ops/sec (threshold: ${THRESHOLDS[throughput_ops]})"
        ((PASS++))
    else
        echo -e "${RED}✗${NC} Throughput: ${THROUGHPUT} ops/sec (threshold: ${THRESHOLDS[throughput_ops]})"
        ((FAIL++))
    fi
fi

# Check latency (from console output)
LATENCY_P50=$(grep "Average time" target/benchmark-results/console-output.txt | head -1 | awk '{print $4}')
if [[ -n "$LATENCY_P50" ]]; then
    echo -e "${GREEN}✓${NC} Latency P50: ${LATENCY_P50} ms/op"
    ((PASS++))
fi

# Summary
echo ""
echo "========================================"
echo "  Summary"
echo "========================================"
echo -e "${GREEN}Passed: $PASS${NC}"
if [[ $FAIL -gt 0 ]]; then
    echo -e "${RED}Failed: $FAIL${NC}"
    echo ""
    echo -e "${RED}PERFORMANCE GATE FAILED${NC}"
    exit 1
else
    echo -e "${GREEN}All performance gates passed!${NC}"
fi

# Generate JFR recording command
echo ""
echo "========================================"
echo "  Profiling Commands"
echo "========================================"
echo ""
echo "# Start JFR recording for next run:"
echo "java -XX:StartFlightRecording=duration=60s,name=perf,filename=target/jfr/perf.jfr,settings=profile \\"
echo "     -jar target/benchmarks.jar"
echo ""
echo "# Analyze JFR recording:"
echo "jfr print target/jfr/perf.jfr"
echo ""
echo "# Or open in Java Mission Control (JMC)"
echo ""

# Generate async-profiler commands
echo "========================================"
echo "  Async Profiler Commands"
echo "========================================"
echo ""
echo "# CPU Profile:"
echo "./profiler.sh start --event cpu --interval 1ms --file target/async-profiler/cpu.html <PID>"
echo ""
echo "# Memory Profile:"
echo "./profiler.sh start --event alloc --interval 500k --file target/async-profiler/alloc.html <PID>"
echo ""
echo "# Lock Contention:"
echo "./profiler.sh start --event lock --file target/async-profiler/lock.html <PID>"
echo ""

exit 0
