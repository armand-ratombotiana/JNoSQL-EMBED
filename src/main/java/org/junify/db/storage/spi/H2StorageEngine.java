package org.junify.db.storage.spi;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class H2StorageEngine implements StorageEngine {

    private final Path dataDir;
    private final String dbName;
    private Connection connection;
    private final ReentrantReadWriteLock lock;
    private int lockTimeoutSeconds = 30;
    private final Map<String, byte[]> cache;
    private final Map<String, PreparedStatement> statementCache;
    private volatile boolean closed;
    private boolean autoCommit;
    private int isolationLevel = Connection.TRANSACTION_READ_COMMITTED;
    private final List<Savepoint> savepoints;
    private SchemaManager schemaManager;
    private QueryOptimizer queryOptimizer;

    public H2StorageEngine(Path dataDir) {
        this(dataDir, "embeddb");
    }

    public H2StorageEngine(Path dataDir, String dbName) {
        this(dataDir, dbName, 30);
    }

    /**
     * Creates H2StorageEngine with custom query timeout.
     * @param dataDir directory for database files
     * @param dbName database name
     * @param queryTimeoutSeconds query timeout in seconds (default: 30)
     */
    public H2StorageEngine(Path dataDir, String dbName, int queryTimeoutSeconds) {
        this.dataDir = dataDir;
        this.dbName = dbName;
        this.queryTimeout = queryTimeoutSeconds;
        this.lock = new ReentrantReadWriteLock(true); // fair lock to prevent starvation
        this.lockTimeoutSeconds = 30;
        this.cache = new ConcurrentHashMap<>();
        this.statementCache = new ConcurrentHashMap<>(50);
        this.savepoints = new ArrayList<>();
        this.closed = false;
        this.autoCommit = true;

        try {
            Files.createDirectories(dataDir);
            initializeDatabase();
            this.schemaManager = new SchemaManager(this);
            this.queryOptimizer = new QueryOptimizer(this);
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
                     ";MODE=MySQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1";

        connection = DriverManager.getConnection(url, "sa", "");
        
        // Register shutdown hook to prevent file locks on improper close
        Runtime.getRuntime().addShutdownHook(new Thread(this::close, "H2-ShutdownHook"));

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
        tryAcquireWriteLock();
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
        tryAcquireWriteLock();
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
        
        tryAcquireReadLock();
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
        tryAcquireWriteLock();
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
        tryAcquireReadLock();
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
        tryAcquireWriteLock();
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
        tryAcquireReadLock();
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
        tryAcquireReadLock();
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
        tryAcquireWriteLock();
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
        tryAcquireReadLock();
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
        tryAcquireWriteLock();
        try {
            cache.clear();
            if (statementCache.size() > 100) {
                statementCache.clear();
            }
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
    public void beginTransaction() {
        try {
            checkOpen();
            connection.setAutoCommit(false);
            autoCommit = false;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to begin transaction", e);
        }
    }

    public void commit() throws SQLException {
        checkOpen();
        if (!autoCommit) {
            connection.commit();
            connection.setAutoCommit(true);
            autoCommit = true;
            savepoints.clear();
        }
    }

    public void rollback() throws SQLException {
        checkOpen();
        if (!autoCommit) {
            connection.rollback();
            connection.setAutoCommit(true);
            autoCommit = true;
            savepoints.clear();
        }
    }

    public void setTransactionIsolation(int level) throws SQLException {
        checkOpen();
        this.isolationLevel = level;
        connection.setTransactionIsolation(level);
    }

    public int getTransactionIsolation() {
        return isolationLevel;
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        checkOpen();
        Savepoint sp = connection.setSavepoint(name);
        savepoints.add(sp);
        return sp;
    }

    public void rollbackTo(Savepoint sp) throws SQLException {
        checkOpen();
        connection.rollback(sp);
        savepoints.remove(sp);
    }

    public void releaseSavepoint(Savepoint sp) throws SQLException {
        checkOpen();
        connection.releaseSavepoint(sp);
        savepoints.remove(sp);
    }

    public SchemaManager schemaManager() {
        return schemaManager;
    }

    public QueryOptimizer queryOptimizer() {
        return queryOptimizer;
    }

    private ConstraintManager constraintManager;
    private WindowFunctionManager windowFunctionManager;
    private CTEAndRecursiveManager cteManager;
    private AnalyticFunctionManager analyticManager;

    public ConstraintManager constraintManager() {
        if (constraintManager == null) {
            constraintManager = new ConstraintManager(this);
        }
        return constraintManager;
    }

    public WindowFunctionManager windowFunctionManager() {
        if (windowFunctionManager == null) {
            windowFunctionManager = new WindowFunctionManager(this);
        }
        return windowFunctionManager;
    }

    public CTEAndRecursiveManager cteManager() {
        if (cteManager == null) {
            cteManager = new CTEAndRecursiveManager(this);
        }
        return cteManager;
    }

    public AnalyticFunctionManager analyticFunctionManager() {
        if (analyticManager == null) {
            analyticManager = new AnalyticFunctionManager(this);
        }
        return analyticManager;
    }

    boolean isAutoCommit() {
        return autoCommit;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try {
            flush();
        } catch (Exception e) {
            System.err.println("Warning: Failed to flush H2 storage engine: " + e.getMessage());
        }
        tryAcquireWriteLock();
        try {
            if (connection != null && !connection.isClosed()) {
                try {
                    // Commit any pending transactions before closing
                    if (!autoCommit) {
                        connection.commit();
                    }
                } catch (SQLException e) {
                    System.err.println("Warning: Failed to commit: " + e.getMessage());
                }
                connection.close();
                System.out.println("[H2StorageEngine] Database connection closed successfully");
            }
        } catch (SQLException e) {
            System.err.println("Warning: Failed to close H2 connection: " + e.getMessage());
        } finally {
            lock.writeLock().unlock();
        }
    }

    
    /**
     * Acquire write lock with timeout to detect potential deadlocks.
     * @throws RuntimeException if lock cannot be acquired within timeout
     */
    private boolean tryAcquireWriteLock() {
        try {
            if (!lock.writeLock().tryLock(lockTimeoutSeconds, TimeUnit.SECONDS)) {
                System.err.println("[H2StorageEngine] Write lock timeout - potential deadlock detected");
                logLockState();
                throw new LockTimeoutException("Write lock timeout after " + lockTimeoutSeconds + "s - potential deadlock");
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Lock acquisition interrupted", e);
        }
    }

    /**
     * Acquire read lock with timeout to detect potential deadlocks.
     * @throws RuntimeException if lock cannot be acquired within timeout
     */
    private boolean tryAcquireReadLock() {
        try {
            if (!lock.readLock().tryLock(lockTimeoutSeconds, TimeUnit.SECONDS)) {
                System.err.println("[H2StorageEngine] Read lock timeout - potential deadlock detected");
                logLockState();
                throw new LockTimeoutException("Read lock timeout after " + lockTimeoutSeconds + "s - potential deadlock");
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Lock acquisition interrupted", e);
        }
    }

    /**
     * Log current lock state for debugging.
     */
    private void logLockState() {
        System.err.println("[H2StorageEngine] Lock state: " +
            "WriteLocked=" + lock.isWriteLocked() +
            ", WriteHoldCount=" + lock.getWriteHoldCount() +
            ", ReadHoldCount=" + lock.getReadHoldCount() +
            ", HasQueuedThreads=" + lock.hasQueuedThreads());
    }

    /**
     * Custom exception for lock timeouts.
     */
    public static class LockTimeoutException extends RuntimeException {
        public LockTimeoutException(String message) {
            super(message);
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

    private int queryTimeout = 30;
    private static final int STATEMENT_CACHE_MAX_SIZE = 256;

    public void setQueryTimeout(int seconds) {
        this.queryTimeout = seconds;
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

    public SqlResult executeSql(String sql) {
        return executeSql(sql, false);
    }

    /**
     * Executes SQL with parameterized values to prevent SQL injection.
     * Supports String, Number, Boolean, Date, Time, Timestamp, byte[] parameters.
     *
     * @param sql SQL with ? placeholders
     * @param params parameter values to bind
     * @return SqlResult with execution outcome
     */
    public SqlResult executeSql(String sql, Object... params) {
        return executeSql(sql, false, params);
    }

    /**
     * Executes SQL with optional EXPLAIN analysis and parameterized values.
     * Uses PreparedStatement for SELECT/INSERT/UPDATE/DELETE with parameters.
     * Falls back to Statement for SQL without parameters or multi-statement SQL.
     *
     * @param sql SQL statement (with ? placeholders for parameters)
     * @param analyze if true, prepends EXPLAIN for query plan
     * @param params parameter values to bind (optional)
     * @return SqlResult with execution outcome
     */
    public SqlResult executeSql(String sql, boolean analyze, Object... params) {
        checkOpen();
        if (sql == null || sql.trim().isEmpty()) {
            return new SqlResult(false, null, 0, "Empty SQL statement");
        }

        String originalSql = sql;
        if (analyze) {
            sql = "EXPLAIN " + sql;
        }

        String trimmed = sql.trim().toUpperCase();
        boolean hasParams = params != null && params.length > 0;
        boolean isMultiStatement = trimmed.contains(";") && !trimmed.startsWith("SELECT") && !trimmed.startsWith("EXPLAIN");
        boolean isSelectQuery = trimmed.startsWith("SELECT") || trimmed.startsWith("EXPLAIN");
        boolean isWriteQuery = trimmed.startsWith("INSERT") || trimmed.startsWith("UPDATE") || 
                               trimmed.startsWith("DELETE") || trimmed.startsWith("MERGE") ||
                               trimmed.startsWith("CREATE") || trimmed.startsWith("DROP") ||
                               trimmed.startsWith("ALTER") || trimmed.startsWith("TRUNCATE");

        // Use write lock for write operations, read lock for reads
        if (isWriteQuery) {
            tryAcquireWriteLock();
        } else {
            tryAcquireReadLock();
        }
        
        try {
            if (isSelectQuery) {
                if (hasParams) {
                    PreparedStatement ps = getCachedPreparedStatement(sql);
                    try {
                        bindParameters(ps, params);
                        try (ResultSet rs = ps.executeQuery()) {
                            return extractResultSet(rs);
                        }
                    } finally {
                        if (!isCached(ps)) {
                            ps.close();
                        }
                    }
                } else {
                    PreparedStatement ps = getCachedPreparedStatement(sql);
                    try {
                        try (ResultSet rs = ps.executeQuery()) {
                            return extractResultSet(rs);
                        }
                    } finally {
                        if (!isCached(ps)) {
                            ps.close();
                        }
                    }
                }
            } else if (hasParams && !isMultiStatement) {
                // Use PreparedStatement for parameterized non-SELECT statements
                PreparedStatement ps = getCachedPreparedStatement(originalSql);
                try {
                    bindParameters(ps, params);
                    int affected = ps.executeUpdate();
                    return new SqlResult(true, null, affected, affected + " row(s) affected");
                } finally {
                    if (!isCached(ps)) {
                        ps.close();
                    }
                }
            } else {
                // Fallback to Statement for multi-statement or no-params SQL
                int affected = 0;
                if (isMultiStatement) {
                    String[] statements = sql.split(";");
                    for (String stmt : statements) {
                        if (!stmt.trim().isEmpty()) {
                            try (Statement s = connection.createStatement()) {
                                s.setQueryTimeout(queryTimeout);
                                affected += s.executeUpdate(stmt.trim());
                            }
                        }
                    }
                } else {
                    try (Statement s = connection.createStatement()) {
                        s.setQueryTimeout(queryTimeout);
                        affected = s.executeUpdate(sql);
                    }
                }
                return new SqlResult(true, null, affected, affected + " row(s) affected");
            }
        } catch (SQLException e) {
            return handleSqlException(e, originalSql);
        } finally {
            if (isWriteQuery) {
                lock.writeLock().unlock();
            } else {
                lock.readLock().unlock();
            }
        }
    }

    /**
     * Retrieves a PreparedStatement from cache or creates a new one.
     * Implements LRU eviction when cache exceeds max size.
     */
    private PreparedStatement getCachedPreparedStatement(String sql) throws SQLException {
        PreparedStatement ps = statementCache.get(sql);
        if (ps == null || isStatementClosed(ps)) {
            ps = connection.prepareStatement(sql);
            ps.setQueryTimeout(queryTimeout);
            
            if (statementCache.size() >= STATEMENT_CACHE_MAX_SIZE) {
                // Simple eviction: remove oldest 10% of entries
                int toRemove = STATEMENT_CACHE_MAX_SIZE / 10;
                statementCache.keySet().stream()
                    .limit(toRemove)
                    .forEach(key -> {
                        try {
                            PreparedStatement old = statementCache.remove(key);
                            if (old != null && !isStatementClosed(old)) {
                                old.close();
                            }
                        } catch (SQLException e) {
                            // Ignore close errors during eviction
                        }
                    });
            }
            
            statementCache.put(sql, ps);
        }
        return ps;
    }

    /**
     * Binds parameters to PreparedStatement with type detection.
     * Supports: String, Integer, Long, Double, Float, Boolean, Date, Time, Timestamp, byte[]
     */
    private void bindParameters(PreparedStatement ps, Object[] params) throws SQLException {
        for (int i = 0; i < params.length; i++) {
            Object param = params[i];
            int paramIndex = i + 1;
            
            if (param == null) {
                ps.setNull(paramIndex, Types.NULL);
            } else if (param instanceof String) {
                ps.setString(paramIndex, (String) param);
            } else if (param instanceof Integer) {
                ps.setInt(paramIndex, (Integer) param);
            } else if (param instanceof Long) {
                ps.setLong(paramIndex, (Long) param);
            } else if (param instanceof Double) {
                ps.setDouble(paramIndex, (Double) param);
            } else if (param instanceof Float) {
                ps.setFloat(paramIndex, (Float) param);
            } else if (param instanceof Boolean) {
                ps.setBoolean(paramIndex, (Boolean) param);
            } else if (param instanceof java.util.Date) {
                ps.setTimestamp(paramIndex, new java.sql.Timestamp(((java.util.Date) param).getTime()));
            } else if (param instanceof java.sql.Date) {
                ps.setDate(paramIndex, (java.sql.Date) param);
            } else if (param instanceof java.sql.Time) {
                ps.setTime(paramIndex, (java.sql.Time) param);
            } else if (param instanceof java.sql.Timestamp) {
                ps.setTimestamp(paramIndex, (java.sql.Timestamp) param);
            } else if (param instanceof byte[]) {
                ps.setBytes(paramIndex, (byte[]) param);
            } else if (param instanceof java.io.Reader) {
                ps.setCharacterStream(paramIndex, (java.io.Reader) param);
            } else if (param instanceof java.io.InputStream) {
                ps.setBinaryStream(paramIndex, (java.io.InputStream) param);
            } else {
                // Fallback: use toString() for unknown types
                ps.setString(paramIndex, param.toString());
            }
        }
    }

    /**
     * Extracts ResultSet metadata and rows into SqlResult.
     * Column names are stored in uppercase for consistency.
     */
    private SqlResult extractResultSet(ResultSet rs) throws SQLException {
        var columns = new java.util.ArrayList<String>();
        var meta = rs.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            String colLabel = meta.getColumnLabel(i);
            columns.add(colLabel != null ? colLabel.toUpperCase() : meta.getColumnName(i).toUpperCase());
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

    /**
     * Checks if a PreparedStatement is closed or invalid.
     */
    private boolean isStatementClosed(PreparedStatement ps) {
        if (ps == null) return true;
        try {
            return ps.isClosed();
        } catch (SQLException e) {
            return true;
        }
    }

    /**
     * Checks if a PreparedStatement is in the cache.
     */
    private boolean isCached(PreparedStatement ps) {
        return statementCache.containsValue(ps);
    }

    /**
     * Handles SQLException by classifying it and returning structured error result.
     *
     * @param e the SQLException to handle
     * @param originalSql the SQL statement that caused the error
     * @return SqlResult with classified error information
     */
    private SqlResult handleSqlException(SQLException e, String originalSql) {
        String sqlState = e.getSQLState() != null ? e.getSQLState() : "";
        SqlErrorCode errorCode = SqlErrorCode.fromSqlState(sqlState);
        String suggestion = buildSuggestion(errorCode, e.getMessage(), originalSql);

        // Log detailed error for debugging
        System.err.println("[SQL Error] Code: " + errorCode.getCode() +
                          ", State: " + sqlState +
                          ", Message: " + e.getMessage());

        return new SqlResult(false, null, 0, e.getMessage(),
                            errorCode.getCode(), sqlState, suggestion, originalSql);
    }

    /**
     * Builds actionable suggestion based on error code and message.
     *
     * @param errorCode the classified error code
     * @param message the error message from database
     * @param originalSql the SQL statement that failed
     * @return actionable suggestion string
     */
    private String buildSuggestion(SqlErrorCode errorCode, String message, String originalSql) {
        StringBuilder suggestion = new StringBuilder(errorCode.getSuggestion());

        // Add specific details based on error type
        if (errorCode == SqlErrorCode.SYNTAX_ERROR && message != null) {
            // Extract position information if available
            if (message.contains("line")) {
                suggestion.append(" Error location: ").append(extractLineInfo(message));
            }
        } else if (errorCode == SqlErrorCode.NOT_FOUND && message != null) {
            // Suggest available tables/columns
            if (message.toLowerCase().contains("table")) {
                suggestion.append(" Available tables: " + String.join(", ", schemaManager().getTables()));
            }
        } else if (errorCode == SqlErrorCode.CONSTRAINT_VIOLATION && message != null) {
            // Extract constraint name if present
            if (message.contains("PRIMARY_KEY") || message.contains("unique constraint")) {
                suggestion.append(" A record with this key already exists.");
            } else if (message.contains("FOREIGN_KEY") || message.contains("referential")) {
                suggestion.append(" Referenced record does not exist.");
            }
        } else if (errorCode == SqlErrorCode.TIMEOUT) {
            suggestion.append(" Current timeout: " + queryTimeout + " seconds.");
        }

        return suggestion.toString();
    }

    /**
     * Extract line information from error message.
     */
    private String extractLineInfo(String message) {
        // Try to extract "line X" or "position X" from message
        int lineIdx = message.toLowerCase().indexOf("line");
        if (lineIdx >= 0 && lineIdx + 4 < message.length()) {
            int start = lineIdx + 5;
            int end = start;
            while (end < message.length() && Character.isDigit(message.charAt(end))) {
                end++;
            }
            if (end > start) {
                return message.substring(lineIdx, end);
            }
        }
        return "";
    }

    /**
     * Clears all cached PreparedStatements.
     * Call during maintenance windows or when schema changes.
     */
    public void clearStatementCache() {
        tryAcquireWriteLock();
        try {
            statementCache.forEach((sql, ps) -> {
                try {
                    if (!isStatementClosed(ps)) {
                        ps.close();
                    }
                } catch (SQLException e) {
                    // Ignore close errors
                }
            });
            statementCache.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public SqlResult executeSql(String sql, boolean analyze) {
        return executeSql(sql, analyze, new Object[0]);
    }

    /**
     * SqlResult with enhanced error information.
     * Contains error code, SQL state, suggestion, and original SQL for structured error responses.
     */
    public record SqlResult(
        boolean success,
        java.util.List<String> columns,
        int affected,
        String message,
        java.util.List<java.util.Map<String, Object>> rows,
        java.util.List<String> allColumns,
        String errorCode,
        String sqlState,
        String suggestion,
        String originalSql
    ) {
        public SqlResult(boolean success, java.util.List<String> columns, int affected, String message) {
            this(success, columns, affected, message, null, columns, null, null, null, null);
        }

        public SqlResult(boolean success, java.util.List<String> columns, int affected, String message,
                        String errorCode, String sqlState, String suggestion, String originalSql) {
            this(success, columns, affected, message, null, columns, errorCode, sqlState, suggestion, originalSql);
        }

        public SqlResult(boolean success, java.util.List<String> columns, int affected, String message,
                        java.util.List<java.util.Map<String, Object>> rows, java.util.List<String> allColumns) {
            this(success, columns, affected, message, rows, allColumns, null, null, null, null);
        }

        /**
         * Convert to JSON error response.
         * Returns null if operation was successful.
         */
        public String toJsonError() {
            if (success || errorCode == null) {
                return null;
            }
            StringBuilder json = new StringBuilder("{\n  \"error\": {\n");
            json.append("    \"code\": \"").append(escapeJson(errorCode)).append("\",\n");
            json.append("    \"message\": \"").append(escapeJson(message)).append("\",\n");
            if (sqlState != null) {
                json.append("    \"sqlState\": \"").append(escapeJson(sqlState)).append("\",\n");
            }
            if (suggestion != null) {
                json.append("    \"suggestion\": \"").append(escapeJson(suggestion)).append("\",\n");
            }
            if (originalSql != null) {
                json.append("    \"originalSql\": \"").append(escapeJson(originalSql)).append("\"\n");
            } else {
                json.append("    \"originalSql\": null\n");
            }
            json.append("  }\n}");
            return json.toString();
        }

        /**
         * Convert to structured error map for API responses.
         * Returns null if operation was successful.
         */
        public java.util.Map<String, Object> toErrorMap() {
            if (success || errorCode == null) {
                return null;
            }
            java.util.Map<String, Object> error = new java.util.HashMap<>();
            error.put("code", errorCode);
            error.put("message", message);
            if (sqlState != null) {
                error.put("sqlState", sqlState);
            }
            if (suggestion != null) {
                error.put("suggestion", suggestion);
            }
            if (originalSql != null) {
                error.put("originalSql", originalSql);
            }
            return error;
        }

        private String escapeJson(String value) {
            if (value == null) return "";
            return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
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
        tryAcquireReadLock();
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
        var map = new java.util.HashMap<String, Object>();
        map.put("engine", name());
        map.put("documents", docCount);
        map.put("keyValues", kvCount);
        map.put("columns", colCount);
        map.put("totalEntries", docCount + kvCount + colCount);
        map.put("cacheSize", cache.size());
        map.put("statementCacheSize", statementCache.size());
        map.put("dataDir", dataDir.toString());
        map.put("autoCommit", autoCommit);
        map.put("isolationLevel", isolationLevelName(isolationLevel));
        map.put("type", "h2-sql");
        return map;
    }

    private String isolationLevelName(int level) {
        return switch (level) {
            case Connection.TRANSACTION_NONE -> "NONE";
            case Connection.TRANSACTION_READ_UNCOMMITTED -> "READ_UNCOMMITTED";
            case Connection.TRANSACTION_READ_COMMITTED -> "READ_COMMITTED";
            case Connection.TRANSACTION_REPEATABLE_READ -> "REPEATABLE_READ";
            case Connection.TRANSACTION_SERIALIZABLE -> "SERIALIZABLE";
            default -> "UNKNOWN";
        };
    }
}
