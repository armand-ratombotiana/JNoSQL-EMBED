#!/bin/bash
#
# JunifyDB Performance Baseline Script
# Runs all JMH benchmarks and validates against SPEC.md targets
#
# Usage:
#   ./perf-baseline.sh [full|quick|storage|mvcc|wal|vector]
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# SPEC.md Performance Targets
TARGET_KV_P50_LATENCY_MS=1.0
TARGET_INDEXED_P50_LATENCY_MS=5.0
TARGET_HYBRID_P99_LATENCY_MS=50.0
TARGET_THROUGHPUT_OPS_SEC=50000
TARGET_STARTUP_MS=200
TARGET_HEAP_MB=50
TARGET_GC_PAUSE_MS=10

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  JunifyDB Performance Baseline Suite  ${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "Java Version: ${YELLOW}$(java -version 2>&1 | head -n 1)${NC}"
echo -e "JVM: ${YELLOW}$(java -XshowSettings:properties -version 2>&1 | grep 'java.vm.name' | cut -d'=' -f2)${NC}"
echo ""

# Function to run benchmarks
run_benchmarks() {
    local profile=$1
    local include=$2
    
    echo -e "${BLUE}Running benchmarks: ${YELLOW}$profile${NC}"
    echo ""
    
    # Create output directory
    mkdir -p target/benchmark-results
    mkdir -p target/jfr
    mkdir -p target/async-profiler
    
    # Build with benchmark profile
    echo -e "${YELLOW}Building benchmarks...${NC}"
    mvn clean package -Pbenchmark -DskipTests -q
    
    # Run benchmarks
    echo -e "${YELLOW}Executing JMH benchmarks...${NC}"
    echo ""
    
    java --enable-preview \
         -jar target/benchmarks.jar \
         "$include" \
         -r 1 \
         -wi 3 \
         -i 5 \
         -f 1 \
         -rf json \
         -rff target/benchmark-results/$profile-results.json
    
    echo ""
    echo -e "${GREEN}Benchmark complete. Results saved to target/benchmark-results/$profile-results.json${NC}"
}

# Function to run with JFR profiling
run_with_jfr() {
    local profile=$1
    local include=$2
    
    echo -e "${BLUE}Running with JFR profiling: ${YELLOW}$profile${NC}"
    echo ""
    
    java --enable-preview \
         -XX:StartFlightRecording=duration=60s,name=$profile,filename=target/jfr/$profile.jfr,settings=profile \
         -XX:FlightRecorderOptions=stackdepth=256,dumponexit=true \
         -jar target/benchmarks.jar \
         "$include" \
         -r 1 \
         -wi 3 \
         -i 5 \
         -f 1
    
    echo -e "${GREEN}JFR recording saved to target/jfr/$profile.jfr${NC}"
    echo -e "Analyze with: ${YELLOW}jfr print target/jfr/$profile.jfr${NC}"
    echo -e "Or open in Java Mission Control (JMC)"
}

# Function to print GC analysis
print_gc_info() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}  GC Analysis Guide                    ${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
    echo "To enable GC logging for detailed analysis:"
    echo "  -Xlog:gc*:file=target/gc.log:time,uptime,level,tags"
    echo ""
    echo "Upload gc.log to https://gceasy.io for visualization"
    echo ""
    echo -e "SPEC.md GC Targets:"
    echo "  - Young Gen pause: ${YELLOW}<${TARGET_GC_PAUSE_MS}ms${NC}"
    echo "  - GC throughput: ${YELLOW}>95%${NC}"
    echo ""
}

# Function to validate results
validate_results() {
    local results_file=$1
    
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}  SPEC.md Validation                   ${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo ""
    
    if [ ! -f "$results_file" ]; then
        echo -e "${RED}Results file not found: $results_file${NC}"
        return 1
    fi
    
    echo -e "Results file: ${YELLOW}$results_file${NC}"
    echo ""
    echo "Key metrics to check:"
    echo "  - KV operations throughput (ops/sec)"
    echo "  - Document query latency (ms/op)"
    echo "  - Vector search latency (ms/op)"
    echo "  - Transaction overhead (us/op)"
    echo ""
    echo -e "SPEC.md Targets:"
    echo "  - KV p50 latency: ${YELLOW}<${TARGET_KV_P50_LATENCY_MS}ms${NC}"
    echo "  - Indexed p50 latency: ${YELLOW}<${TARGET_INDEXED_P50_LATENCY_MS}ms${NC}"
    echo "  - Hybrid/vector p99 latency: ${YELLOW}<${TARGET_HYBRID_P99_LATENCY_MS}ms${NC}"
    echo "  - Throughput: ${YELLOW}>${TARGET_THROUGHPUT_OPS_SEC} ops/sec${NC}"
    echo "  - Startup: ${YELLOW}<${TARGET_STARTUP_MS}ms${NC}"
    echo "  - Heap idle: ${YELLOW}<${TARGET_HEAP_MB}MB${NC}"
    echo ""
}

# Main execution
case "${1:-full}" in
    full)
        run_benchmarks "storage" "StorageEngineBenchmark"
        run_benchmarks "mvcc" "MVCCBenchmark"
        run_benchmarks "wal" "WALBenchmark"
        run_benchmarks "vector" "VectorSearchBenchmark"
        validate_results "target/benchmark-results/storage-results.json"
        print_gc_info
        ;;
    quick)
        run_benchmarks "quick" "StorageEngineBenchmark.kv"
        validate_results "target/benchmark-results/quick-results.json"
        ;;
    storage)
        run_benchmarks "storage" "StorageEngineBenchmark"
        validate_results "target/benchmark-results/storage-results.json"
        ;;
    mvcc)
        run_benchmarks "mvcc" "MVCCBenchmark"
        validate_results "target/benchmark-results/mvcc-results.json"
        ;;
    wal)
        run_benchmarks "wal" "WALBenchmark"
        validate_results "target/benchmark-results/wal-results.json"
        ;;
    vector)
        run_benchmarks "vector" "VectorSearchBenchmark"
        validate_results "target/benchmark-results/vector-results.json"
        ;;
    jfr)
        run_with_jfr "storage" "StorageEngineBenchmark"
        run_with_jfr "mvcc" "MVCCBenchmark"
        ;;
    help)
        echo "Usage: $0 [full|quick|storage|mvcc|wal|vector|jfr|help]"
        echo ""
        echo "  full    - Run all benchmarks (default)"
        echo "  quick   - Quick benchmark run"
        echo "  storage - Storage engine benchmarks"
        echo "  mvcc    - MVCC transaction benchmarks"
        echo "  wal     - WAL benchmarks"
        echo "  vector  - Vector search benchmarks"
        echo "  jfr     - Run with JFR profiling"
        echo "  help    - Show this help"
        ;;
    *)
        echo "Unknown option: $1"
        echo "Use '$0 help' for usage"
        exit 1
        ;;
esac

echo ""
echo -e "${GREEN}=========================================${NC}"
echo -e "${GREEN}  Performance Baseline Complete         ${NC}"
echo -e "${GREEN}=========================================${NC}"
