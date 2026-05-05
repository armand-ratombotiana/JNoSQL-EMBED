package org.junify.db.storage.spi;

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

    public ReplicationManager(H2StorageEngine engine, String nodeId, String masterUrl) {
        this.engine = engine;
        this.nodeId = nodeId;
        this.masterUrl = masterUrl;
        this.eventQueue = new LinkedBlockingQueue<>(10000);
        this.running = new AtomicBoolean(false);
        this.slots = new ConcurrentHashMap<>();
        this.isMaster = masterUrl == null || masterUrl.isEmpty();
    }

    public void start() {
        if (running.getAndSet(true)) return;
        
        replicator = Executors.newSingleThreadExecutor(r -> {
            var t = new Thread(r, "Replicator-" + nodeId);
            t.setDaemon(true);
            return t;
        });

        replicator.submit(this::replicationLoop);
        
        if (!isMaster) {
            replicator.submit(this::receiveLoop);
        }
    }

    public void stop() {
        running.set(false);
        if (replicator != null) {
            replicator.shutdown();
            try {
                replicator.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void replicationLoop() {
        while (running.get()) {
            try {
                var event = eventQueue.poll(1, TimeUnit.SECONDS);
                if (event == null) continue;

                if (!isMaster && masterUrl != null) {
                    sendToMaster(event);
                }

                for (var slot : slots.values()) {
                    if (slot.replicate) {
                        try {
                            slot.forward(event);
                        } catch (Exception e) {
                            System.err.println("Replication forward error: " + e.getMessage());
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void sendToMaster(ReplicationEvent event) {
        try {
            var url = new URL(masterUrl + "/api/replicate");
            var conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");
            
            try (var out = conn.getOutputStream()) {
                out.write(event.toJson().getBytes());
            }
            
            conn.getResponseCode();
        } catch (Exception e) {
            System.err.println("Failed to send to master: " + e.getMessage());
        }
    }

    private void receiveLoop() {
        while (running.get()) {
            try {
                Thread.sleep(1000);
                
                if (masterUrl != null) {
                    fetchFromMaster();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("Replication receive error: " + e.getMessage());
            }
        }
    }

    private void fetchFromMaster() {
        try {
            var url = new URL(masterUrl + "/api/replicate?nodeId=" + nodeId);
            var conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            
            if (conn.getResponseCode() == 200) {
                try (var in = conn.getInputStream()) {
                    var json = new String(in.readAllBytes());
                    var event = ReplicationEvent.fromJson(json);
                    if (event != null) {
                        applyEvent(event);
                    }
                }
            }
        } catch (Exception e) {
            // Silent fail on fetch
        }
    }

    private void applyEvent(ReplicationEvent event) {
        switch (event.type()) {
            case "INSERT", "UPDATE" -> engine.executeSql(event.sql());
            case "DELETE" -> {
                var sql = "DELETE " + event.table() + " WHERE " + event.keyColumn() + " = '" + event.keyValue() + "'";
                engine.executeSql(sql);
            }
        }
    }

    public void recordChange(String type, String table, String sql, String keyColumn, String keyValue) {
        var event = new ReplicationEvent(
            System.currentTimeMillis(),
            nodeId,
            type,
            table,
            sql,
            keyColumn,
            keyValue
        );
        eventQueue.offer(event);
    }

    public SqlResult addReplica(String url, boolean replicate) {
        slots.put(url, new ReplicationSlot(url, replicate));
        return new SqlResult(true, null, 0, "Replica added: " + url);
    }

    public SqlResult removeReplica(String url) {
        slots.remove(url);
        return new SqlResult(true, null, 0, "Replica removed: " + url);
    }

    public List<String> getReplicas() {
        return new ArrayList<>(slots.keySet());
    }

    public Map<String, Object> getStatus() {
        return Map.of(
            "nodeId", nodeId,
            "role", isMaster ? "MASTER" : "SLAVE",
            "master", masterUrl != null ? masterUrl : "N/A",
            "queueSize", eventQueue.size(),
            "slots", slots.size(),
            "running", running.get()
        );
    }

    public record ReplicationSlot(String url, boolean replicate) {
        void forward(ReplicationEvent event) throws Exception {
            var conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.getOutputStream().write(event.toJson().getBytes());
            conn.getResponseCode();
        }
    }

    public record ReplicationEvent(long timestamp, String nodeId, String type, String table, 
                               String sql, String keyColumn, String keyValue) {
        String toJson() {
            return String.format("{\"ts\":%d,\"node\":\"%s\",\"type\":\"%s\",\"table\":\"%s\",\"sql\":\"%s\",\"kc\":\"%s\",\"kv\":\"%s\"}",
                timestamp, nodeId, type, table, sql.replace("\"", "'"), keyColumn, keyValue);
        }

        static ReplicationEvent fromJson(String json) {
            try {
                var parts = json.replaceAll("[{}\"]", "").split(",");
                var map = new java.util.HashMap<String, String>();
                for (var part : parts) {
                    var kv = part.split(":");
                    if (kv.length == 2) map.put(kv[0], kv[1]);
                }
                return new ReplicationEvent(
                    Long.parseLong(map.get("ts")),
                    map.get("node"),
                    map.get("type"),
                    map.get("table"),
                    map.get("sql"),
                    map.get("kc"),
                    map.get("kv")
                );
            } catch (Exception e) {
                return null;
            }
        }
    }

    }