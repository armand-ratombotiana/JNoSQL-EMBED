package org.junify.db.storage.spi;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class H2StorageEngine implements StorageEngine {

    private final Path dataDir;
    private final String dbName;
    private Connection connection;
    private final ReentrantReadWriteLock lock;
    private final Map<String, byte[]> cache;
    private volatile boolean closed;

    public H2StorageEngine(Path dataDir) {
        this(dataDir, "embeddb");
    }

    public H2StorageEngine(Path dataDir, String dbName) {
        this.dataDir = dataDir;
        this.dbName = dbName;
        this.lock = new ReentrantReadWriteLock();
        this.cache = new ConcurrentHashMap<>();
        this.closed = false;
        
        try {
            Files.createDirectories(dataDir);
            initializeDatabase();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize H2 database", e);
        }
    }

    @Override
    public String name() {
        return "H2";
    }

    private void initializeDatabase() throws SQLException {
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            System.err.println("H2 Driver not found. Make sure h2 dependency is added.");
        }
        
        String dbPath = dataDir.resolve(dbName).toAbsolutePath().toString();
        String url = "jdbc:h2:file:" + dbPath + 
                     ";MODE=MySQL;DATABASE_TO_LOWER=TRUE";
        
        connection = DriverManager.getConnection(url, "sa", "");
        
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                "CREATE TABLE IF NOT EXISTS kv_store (" +
                "collection VARCHAR(255), " +
                "key_name VARCHAR(255), " +
                "kv_value CLOB, " +
                "expires_at BIGINT, " +
                "PRIMARY KEY (collection, key_name))"
            );
            stmt.execute(
                "CREATE TABLE IF NOT EXISTS doc_store (" +
                "collection VARCHAR(255), " +
                "doc_id VARCHAR(255), " +
                "content TEXT, " +
                "expires_at BIGINT, " +
                "PRIMARY KEY (collection, doc_id))"
            );
            stmt.execute(
                "CREATE TABLE IF NOT EXISTS col_store (" +
                "family VARCHAR(255), " +
                "row_key VARCHAR(255), " +
                "columns TEXT, " +
                "expires_at BIGINT, " +
                "PRIMARY KEY (family, row_key))"
            );
            stmt.execute(
                "CREATE TABLE IF NOT EXISTS meta_store (" +
                "meta_key VARCHAR(255) PRIMARY KEY, " +
                "meta_value VARCHAR(1000))"
            );
            
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_kv_expire ON kv_store(expires_at)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_doc_expire ON doc_store(expires_at)");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_col_expire ON col_store(expires_at)");
        }
    }

    @Override
    public void put(String collection, String key, String value) {
        checkOpen();
        lock.writeLock().lock();
        try {
            String sql = "MERGE INTO kv_store (collection, key_name, kv_value, expires_at) KEY(collection, key_name) VALUES (?, ?, ?, NULL)";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, collection);
                ps.setString(2, key);
                ps.setString(3, value);
                ps.executeUpdate();
            }
            cache.put(collection + ":" + key, value.getBytes());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to put key-value", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void putAll(String collection, Map<String, String> entries) {
        lock.writeLock().lock();
        try {
            String sql = "MERGE INTO kv_store (collection, key_name, kv_value, expires_at) KEY(collection, key_name) VALUES (?, ?, ?, NULL)";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                for (var entry : entries.entrySet()) {
                    ps.setString(1, collection);
                    ps.setString(2, entry.getKey());
                    ps.setString(3, entry.getValue());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            for (var entry : entries.entrySet()) {
                cache.put(collection + ":" + entry.getKey(), entry.getValue().getBytes());
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to put all entries", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String get(String collection, String key) {
        checkOpen();
        String cacheKey = collection + ":" + key;
        
        if (cache.containsKey(cacheKey)) {
            return new String(cache.get(cacheKey));
        }
        
        lock.readLock().lock();
        try {
            String sql = "SELECT kv_value FROM kv_store WHERE collection = ? AND key_name = ?";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, collection);
                ps.setString(2, key);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        String value = rs.getString("kv_value");
                        cache.put(cacheKey, value.getBytes());
                        return value;
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get value", e);
        } finally {
            lock.readLock().unlock();
        }
        return null;
    }

    @Override
    public List<String> getAll(String collection, List<String> keys) {
        return keys.stream().map(k -> get(collection, k)).collect(Collectors.toList());
    }

    @Override
    public void delete(String collection, String key) {
        checkOpen();
        lock.writeLock().lock();
        try {
            String sql = "DELETE FROM kv_store WHERE collection = ? AND key_name = ?";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, collection);
                ps.setString(2, key);
                ps.executeUpdate();
            }
            cache.remove(collection + ":" + key);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void deleteAll(String collection, List<String> keys) {
        for (String key : keys) {
            delete(collection, key);
        }
    }

    @Override
    public boolean exists(String collection, String key) {
        return get(collection, key) != null;
    }

    @Override
    public List<String> scan(String collection) {
        checkOpen();
        List<String> results = new ArrayList<>();
        lock.readLock().lock();
        try {
            String sql = "SELECT kv_value FROM kv_store WHERE collection = ?";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, collection);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        results.add(rs.getString("kv_value"));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to scan", e);
        } finally {
            lock.readLock().unlock();
        }
        return results;
    }

    @Override
    public List<String> scan(String collection, Predicate<String> filter) {
        return scan(collection).stream().filter(filter).collect(Collectors.toList());
    }

    @Override
    public Set<String> keys(String collection) {
        return new HashSet<>(scan(collection));
    }

    public void putDocument(String collection, String docId, String content) {
        checkOpen();
        lock.writeLock().lock();
        try {
            String sql = "MERGE INTO doc_store (collection, doc_id, content, expires_at) KEY(collection, doc_id) VALUES (?, ?, ?, NULL)";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, collection);
                ps.setString(2, docId);
                ps.setString(3, content);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to put document", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public String getDocument(String collection, String docId) {
        checkOpen();
        lock.readLock().lock();
        try {
            String sql = "SELECT content FROM doc_store WHERE collection = ? AND doc_id = ?";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, collection);
                ps.setString(2, docId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString("content");
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get document", e);
        } finally {
            lock.readLock().unlock();
        }
        return null;
    }

    public List<String> getAllDocuments(String collection) {
        checkOpen();
        List<String> results = new ArrayList<>();
        lock.readLock().lock();
        try {
            String sql = "SELECT content FROM doc_store WHERE collection = ?";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, collection);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        results.add(rs.getString("content"));
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get all documents", e);
        } finally {
            lock.readLock().unlock();
        }
        return results;
    }

    public void deleteDocument(String collection, String docId) {
        checkOpen();
        lock.writeLock().lock();
        try {
            String sql = "DELETE FROM doc_store WHERE collection = ? AND doc_id = ?";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, collection);
                ps.setString(2, docId);
                ps.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete document", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long countDocuments(String collection) {
        checkOpen();
        lock.readLock().lock();
        try {
            String sql = "SELECT COUNT(*) FROM doc_store WHERE collection = ?";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, collection);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        return rs.getLong(1);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count documents", e);
        } finally {
            lock.readLock().unlock();
        }
        return 0;
    }

    @Override
    public void flush() {
        lock.writeLock().lock();
        try {
            cache.clear();
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("CHECKPOINT");
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to flush", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        flush();
        lock.writeLock().lock();
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to close", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("H2 storage engine is closed");
        }
    }

    public void clearCache() {
        cache.clear();
    }

    public SqlResult executeSql(String sql) {
        checkOpen();
        if (sql == null || sql.trim().isEmpty()) {
            return new SqlResult(false, null, 0, "Empty SQL statement");
        }
        String trimmed = sql.trim().toUpperCase();
        lock.readLock().lock();
        try {
            if (trimmed.startsWith("SELECT")) {
                try (PreparedStatement ps = connection.prepareStatement(sql)) {
                    try (ResultSet rs = ps.executeQuery()) {
                        var columns = new java.util.ArrayList<String>();
                        var meta = rs.getMetaData();
                        for (int i = 1; i <= meta.getColumnCount(); i++) {
                            columns.add(meta.getColumnLabel(i));
                        }
                        var rows = new java.util.ArrayList<java.util.Map<String, Object>>();
                        while (rs.next()) {
                            var row = new java.util.LinkedHashMap<String, Object>();
                            for (int i = 0; i < columns.size(); i++) {
                                Object val = rs.getObject(i + 1);
                                row.put(columns.get(i), val);
                            }
                            rows.add(row);
                        }
                        return new SqlResult(true, columns, rows.size(), "OK", rows, columns);
                    }
                }
            } else {
                int affected = 0;
                if (trimmed.contains(";")) {
                    String[] statements = sql.split(";");
                    for (String stmt : statements) {
                        if (!stmt.trim().isEmpty()) {
                            try (Statement s = connection.createStatement()) {
                                affected += s.executeUpdate(stmt.trim());
                            }
                        }
                    }
                } else {
                    try (Statement s = connection.createStatement()) {
                        affected = s.executeUpdate(sql);
                    }
                }
                return new SqlResult(true, null, affected, affected + " row(s) affected");
            }
        } catch (SQLException e) {
            return new SqlResult(false, null, 0, "SQL Error: " + e.getMessage());
        } finally {
            lock.readLock().unlock();
        }
    }

    public record SqlResult(boolean success, java.util.List<String> columns, int affected, String message, java.util.List<java.util.Map<String, Object>> rows, java.util.List<String> allColumns) {
        public SqlResult(boolean success, java.util.List<String> columns, int affected, String message) {
            this(success, columns, affected, message, null, columns);
        }
    }

    @Override
    public int size() {
        return cache.size();
    }

    @Override
    public java.util.Map<String, Object> stats() {
        long docCount = 0;
        long kvCount = 0;
        long colCount = 0;
        lock.readLock().lock();
        try {
            if (connection != null && !connection.isClosed()) {
                try (Statement s = connection.createStatement()) {
                    try (ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM doc_store")) {
                        if (rs.next()) docCount = rs.getLong(1);
                    }
                    try (ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM kv_store")) {
                        if (rs.next()) kvCount = rs.getLong(1);
                    }
                    try (ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM col_store")) {
                        if (rs.next()) colCount = rs.getLong(1);
                    }
                }
            }
        } catch (SQLException e) {
            // ignore
        } finally {
            lock.readLock().unlock();
        }
        return java.util.Map.of(
            "engine", name(),
            "documents", docCount,
            "keyValues", kvCount,
            "columns", colCount,
            "totalEntries", docCount + kvCount + colCount,
            "cacheSize", cache.size(),
            "dataDir", dataDir.toString(),
            "type", "h2-sql"
        );
    }
}
