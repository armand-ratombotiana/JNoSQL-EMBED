package org.jnosql.embed.health;

import org.jnosql.embed.JNoSQL;
import org.jnosql.embed.document.DocumentCollection;
import org.jnosql.embed.kv.KeyValueBucket;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class HealthCheck {

    private final JNoSQL db;
    private final ScheduledExecutorService scheduler;
    private final List<HealthIndicator> indicators;
    private final Map<String, HealthStatus> lastStatus;
    private final AtomicBoolean running;

    public HealthCheck(JNoSQL db) {
        this.db = db;
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.indicators = new ArrayList<>();
        this.lastStatus = new ConcurrentHashMap<>();
        this.running = new AtomicBoolean(false);
        registerDefaultIndicators();
    }

    private void registerDefaultIndicators() {
        registerIndicator(new HealthIndicator() {
            @Override public String name() { return "database_open"; }
            @Override public CompletableFuture<HealthStatus> check() {
                return CompletableFuture.completedFuture(
                    db.isOpen() ? HealthStatus.healthy() : HealthStatus.unhealthy("Database is closed")
                );
            }
        });

        registerIndicator(new HealthIndicator() {
            @Override public String name() { return "storage_engine"; }
            @Override public CompletableFuture<HealthStatus> check() {
                return CompletableFuture.completedFuture(
                    HealthStatus.healthy().withDetail("engine", db.config().storageEngine().name())
                );
            }
        });
    }

    public void registerIndicator(HealthIndicator indicator) {
        indicators.add(indicator);
    }

    public HealthReport checkAll() {
        var results = new HashMap<String, HealthStatus>();
        var healthy = true;

        for (var indicator : indicators) {
            try {
                var status = indicator.check().get();
                results.put(indicator.name(), status);
                if (status.getState() != State.UP) {
                    healthy = false;
                }
            } catch (Exception e) {
                results.put(indicator.name(), HealthStatus.unhealthy(e.getMessage()));
                healthy = false;
            }
        }

        lastStatus.putAll(results);
        return new HealthReport(healthy, results);
    }

    public CompletableFuture<HealthReport> checkAllAsync() {
        return CompletableFuture.supplyAsync(this::checkAll, scheduler);
    }

    public void startPeriodicCheck(long interval, TimeUnit unit) {
        if (running.compareAndSet(false, true)) {
            scheduler.scheduleAtFixedRate(this::checkAll, interval, interval, unit);
        }
    }

    public void stopPeriodicCheck() {
        running.set(false);
        scheduler.shutdown();
    }

    public HealthStatus getLastStatus(String indicator) {
        return lastStatus.get(indicator);
    }

    public Map<String, HealthStatus> getLastStatuses() {
        return Map.copyOf(lastStatus);
    }

    public void close() {
        stopPeriodicCheck();
    }

    public interface HealthIndicator {
        String name();
        CompletableFuture<HealthStatus> check();
    }

    public enum State { UP, DOWN, UNKNOWN }

    public static class HealthStatus {
        private final State state;
        private final String message;
        private final Map<String, Object> details;

        private HealthStatus(State state, String message, Map<String, Object> details) {
            this.state = state;
            this.message = message;
            this.details = details;
        }

        public static HealthStatus healthy() {
            return new HealthStatus(State.UP, "OK", Map.of());
        }

        public static HealthStatus unhealthy(String message) {
            return new HealthStatus(State.DOWN, message, Map.of());
        }

        public static HealthStatus unknown(String message) {
            return new HealthStatus(State.UNKNOWN, message, Map.of());
        }

        public HealthStatus withDetail(String key, Object value) {
            var newDetails = new HashMap<>(details);
            newDetails.put(key, value);
            return new HealthStatus(state, message, newDetails);
        }

        public State getState() { return state; }
        public String getMessage() { return message; }
        public Map<String, Object> getDetails() { return details; }
    }

    public static class HealthReport {
        private final boolean healthy;
        private final Map<String, HealthStatus> statuses;
        private final long timestamp;

        public HealthReport(boolean healthy, Map<String, HealthStatus> statuses) {
            this.healthy = healthy;
            this.statuses = statuses;
            this.timestamp = System.currentTimeMillis();
        }

        public boolean isHealthy() { return healthy; }
        public Map<String, HealthStatus> getStatuses() { return statuses; }
        public long getTimestamp() { return timestamp; }

        @Override
        public String toString() {
            return String.format("HealthReport{healthy=%s, statuses=%s, timestamp=%d}",
                healthy, statuses, timestamp);
        }
    }

    public static HealthCheck forDatabase(JNoSQL db) {
        return new HealthCheck(db);
    }
}
