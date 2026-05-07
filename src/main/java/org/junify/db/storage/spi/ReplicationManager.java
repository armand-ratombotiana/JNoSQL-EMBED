package org.junify.db.storage.spi;

import org.junify.db.core.util.CircuitBreaker;
import org.junify.db.core.util.RetryWithBackoff;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junify.db.storage.spi.H2StorageEngine.SqlResult;

public class ReplicationManager {

    private final H2StorageEngine engine;
    private final String nodeId;
    private final String masterUrl;
    private ExecutorService replicator;
    private BlockingQueue<ReplicationEvent> eventQueue;
    private final AtomicBoolean running;
    private final Map<String, ReplicationSlot> slots;
    private volatile boolean isMaster;
    
    private final CircuitBreaker<Void> replicationCircuitBreaker;
    private final CircuitBreaker<Void> replicaForwardingCircuitBreaker;
    private final RetryWithBackoff<Void> retryWithBackoff;

    public ReplicationManager(H2StorageEngine engine, String nodeId, String masterUrl) {
        this.engine = engine;
        this.nodeId = nodeId;
        this.masterUrl = masterUrl;
        this.eventQueue = new LinkedBlockingQueue<>(10000);
        this.running = new AtomicBoolean(false);
        this.slots = new ConcurrentHashMap<>();
        this.isMaster = masterUrl == null || masterUrl.isEmpty();
        this.replicationCircuitBreaker = new CircuitBreaker<>("replication", 5, 2, 30000, 3);
        this.replicaForwardingCircuitBreaker = new CircuitBreaker<>("replica-forwarding", 5, 2, 30000, 3);
        this.retryWithBackoff = new RetryWithBackoff<>(3, 1000, 30000, 2.0, true);
    }

    public void start() {
        if (running.getAndSet(true)) return;
        replicator = Executors.newSingleThreadExecutor(r -> {
            var t = new Thread(r, "Replicator-" + nodeId);
            t.setDaemon(true);
            return t;
        });
        replicator.submit(this::replicationLoop);
        if (!isMaster) { replicator.submit(this::receiveLoop); }
    }

    public void stop() {
        running.set(false);
        if (replicator != null) {
            replicator.shutdown();
            try { replicator.awaitTermination(5, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
    }

    private void replicationLoop() {
        while (running.get()) {
            try {
                var event = eventQueue.poll(1, TimeUnit.SECONDS);
                if (event == null) continue;
                if (!isMaster && masterUrl != null) { sendToMaster(event); }
                for (var slot : slots.values()) {
                    if (slot.replicate) {
                        try { slot.forward(event, replicaForwardingCircuitBreaker, retryWithBackoff); }
                        catch (Exception e) { System.err.println("Replication forward error: " + e.getMessage()); }
                    }
                }
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
    }

    private void sendToMaster(ReplicationEvent event) {
        try {
            replicationCircuitBreaker.executeRunnable(() -> {
                retryWithBackoff.executeRunnable(() -> {
                    try {
                        var url = new URL(masterUrl + "/api/replicate");
                        var conn = (HttpURLConnection) url.openConnection();
                        conn.setRequestMethod("POST");
                        conn.setDoOutput(true);
                        conn.setRequestProperty("Content-Type", "application/json");
                        conn.setConnectTimeout(5000);
                        conn.setReadTimeout(10000);
                        try (var out = conn.getOutputStream()) { out.write(event.toJson().getBytes()); }
                        int responseCode = conn.getResponseCode();
                        if (responseCode >= 500) throw new RuntimeException("Server error: " + responseCode);
                    } catch (IOException e) { throw new RuntimeException("Network error: " + e.getMessage(), e); }
                }, ctx -> System.err.println("Replication retry " + ctx.attempt() + "/" + ctx.maxAttempts() + " failed: " + ctx.exception().getMessage()));
            });
        } catch (CircuitBreaker.CircuitBreakerOpenException e) { System.err.println("Circuit breaker OPEN"); }
        catch (Exception e) { System.err.println("Failed to send: " + e.getMessage()); }
    }

    private void receiveLoop() {
        while (running.get()) {
            try { Thread.sleep(1000); if (masterUrl != null) fetchFromMaster(); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
            catch (Exception e) { System.err.println("Replication receive error: " + e.getMessage()); }
        }
    }

    private void fetchFromMaster() {
        try {
            replicationCircuitBreaker.executeRunnable(() -> {
                retryWithBackoff.executeRunnable(() -> {
                    try {
                        var url = new URL(masterUrl + "/api/replicate?nodeId=" + nodeId);
                        var conn = (HttpURLConnection) url.openConnection();
                        conn.setRequestMethod("GET");
                        conn.setConnectTimeout(5000);
                        conn.setReadTimeout(10000);
                        int responseCode = conn.getResponseCode();
                        if (responseCode == 200) {
                            try (var in = conn.getInputStream()) {
                                var json = new String(in.readAllBytes());
                                var event = ReplicationEvent.fromJson(json);
                                if (event != null) applyEvent(event);
                            }
                        } else if (responseCode >= 500) throw new RuntimeException("Server error: " + responseCode);
                    } catch (IOException e) { throw new RuntimeException("Network error: " + e.getMessage(), e); }
                }, ctx -> System.err.println("Fetch retry " + ctx.attempt() + "/" + ctx.maxAttempts() + " failed"));
            });
        } catch (CircuitBreaker.CircuitBreakerOpenException e) { System.err.println("Circuit breaker OPEN"); }
        catch (Exception e) { System.err.println("Failed to fetch: " + e.getMessage()); }
    }

    private void applyEvent(ReplicationEvent event) {
        switch (event.type()) {
            case "INSERT", "UPDATE" -> engine.executeSql(event.sql());
            case "DELETE" -> engine.executeSql("DELETE " + event.table() + " WHERE " + event.keyColumn() + " = '" + event.keyValue() + "'");
        }
    }

    public void recordChange(String type, String table, String sql, String keyColumn, String keyValue) {
        eventQueue.offer(new ReplicationEvent(System.currentTimeMillis(), nodeId, type, table, sql, keyColumn, keyValue));
    }

    public SqlResult addReplica(String url, boolean replicate) { slots.put(url, new ReplicationSlot(url, replicate)); return new SqlResult(true, null, 0, "Replica added"); }
    public SqlResult removeReplica(String url) { slots.remove(url); return new SqlResult(true, null, 0, "Replica removed"); }
    public List<String> getReplicas() { return new ArrayList<>(slots.keySet()); }

    public Map<String, Object> getStatus() {
        var status = new HashMap<String, Object>();
        status.put("nodeId", nodeId); status.put("role", isMaster ? "MASTER" : "SLAVE");
        status.put("master", masterUrl != null ? masterUrl : "N/A");
        status.put("queueSize", eventQueue.size()); status.put("slots", slots.size());
        status.put("running", running.get()); status.put("circuitBreaker", replicationCircuitBreaker.getStats());
        return status;
    }

    public record ReplicationSlot(String url, boolean replicate) {
        void forward(ReplicationEvent event, CircuitBreaker<Void> cb, RetryWithBackoff<Void> retry) {
            cb.executeRunnable(() -> {
                try {
                    retry.executeRunnable(() -> {
                        try {
                            var conn = (HttpURLConnection) new URL(url).openConnection();
                            conn.setRequestMethod("POST"); conn.setDoOutput(true);
                            conn.setConnectTimeout(5000); conn.setReadTimeout(10000);
                            conn.getOutputStream().write(event.toJson().getBytes());
                            if (conn.getResponseCode() >= 500) throw new RuntimeException("Replica error");
                        } catch (IOException e) { throw new RuntimeException("Network error: " + e.getMessage(), e); }
                    });
                } catch (Exception e) { throw new RuntimeException(e); }
            });
        }
    }

    public record ReplicationEvent(long timestamp, String nodeId, String type, String table,
                                   String sql, String keyColumn, String keyValue) {
        String toJson() {
            StringBuilder sb = new StringBuilder("{");
            sb.append("\"ts\":").append(timestamp).append(",");
            sb.append("\"node\":\"").append(nodeId).append("\",");
            sb.append("\"type\":\"").append(type).append("\",");
            sb.append("\"table\":\"").append(table).append("\",");
            sb.append("\"sql\":\"").append(sql.replace("\"", "\\\"")).append("\",");
            sb.append("\"kc\":\"").append(keyColumn).append("\",");
            sb.append("\"kv\":\"").append(keyValue).append("\"");
            sb.append("}");
            return sb.toString();
        }
        static ReplicationEvent fromJson(String json) {
            try {
                var parts = json.replaceAll("[{}\"]", "").split(",");
                var map = new HashMap<String, String>();
                for (var part : parts) {
                    var kv = part.split(":", 2);
                    if (kv.length == 2) map.put(kv[0], kv[1]);
                }
                return new ReplicationEvent(Long.parseLong(map.get("ts")), map.get("node"), map.get("type"),
                    map.get("table"), map.get("sql"), map.get("kc"), map.get("kv"));
            } catch (Exception e) { return null; }
        }
    }
}
