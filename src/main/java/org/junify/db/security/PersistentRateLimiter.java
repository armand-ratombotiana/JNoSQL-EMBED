package org.junify.db.security;

import org.junify.db.storage.spi.H2StorageEngine;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Persistent Rate Limiter
 * 
 * Database-backed rate limiting for authentication:
 * - Survives server restarts
 * - Works in clustered environments
 * - Automatic cleanup of old records
 * - Configurable attempts and lockout duration
 */
public class PersistentRateLimiter {
    
    private final H2StorageEngine engine;
    private final ConcurrentHashMap<String, Long> lockoutCache = new ConcurrentHashMap<>();
    
    private static final int MAX_ATTEMPTS = 5;
    private static final long LOCKOUT_DURATION_MS = 15 * 60 * 1000;  // 15 minutes
    private static final long WINDOW_MS = 30 * 60 * 1000;  // 30 minute window
    
    public PersistentRateLimiter(H2StorageEngine engine) {
        this.engine = engine;
        initializeTable();
    }
    
    private void initializeTable() {
        engine.executeSql("""
            CREATE TABLE IF NOT EXISTS auth_rate_limits (
                identifier VARCHAR(255) PRIMARY KEY,
                attempt_count INT DEFAULT 0,
                first_attempt_at BIGINT,
                locked_until BIGINT,
                updated_at BIGINT
            )
        """);
        
        // Start cleanup thread
        scheduleCleanup();
    }
    
    /**
     * Check if request is allowed
     */
    public boolean isAllowed(String identifier) {
        // Check in-memory cache first
        Long lockedUntil = lockoutCache.get(identifier);
        if (lockedUntil != null && System.currentTimeMillis() < lockedUntil) {
            return false;
        }
        
        // Check database
        var result = engine.executeSql(
            "SELECT attempt_count, first_attempt_at, locked_until FROM auth_rate_limits WHERE identifier = ?",
            identifier
        );
        
        if (!result.success() || result.rows() == null || result.rows().isEmpty()) {
            return true;  // No record, allow
        }
        
        var row = result.rows().get(0);
        long lockedUntilDb = row.get("LOCKED_UNTIL") != null ? 
            ((Number) row.get("LOCKED_UNTIL")).longValue() : 0;
        
        if (lockedUntilDb > System.currentTimeMillis()) {
            lockoutCache.put(identifier, lockedUntilDb);
            return false;
        }
        
        int attempts = ((Number) row.get("ATTEMPT_COUNT")).intValue();
        long firstAttempt = ((Number) row.get("FIRST_ATTEMPT_AT")).longValue();
        
        // Reset if window expired
        if (System.currentTimeMillis() - firstAttempt > WINDOW_MS) {
            resetAttempts(identifier);
            return true;
        }
        
        return attempts < MAX_ATTEMPTS;
    }
    
    /**
     * Record a failed attempt
     */
    public void recordAttempt(String identifier) {
        var result = engine.executeSql(
            "SELECT attempt_count, first_attempt_at FROM auth_rate_limits WHERE identifier = ?",
            identifier
        );
        
        long now = System.currentTimeMillis();
        
        if (!result.success() || result.rows() == null || result.rows().isEmpty()) {
            // First attempt
            engine.executeSql("""
                INSERT INTO auth_rate_limits (identifier, attempt_count, first_attempt_at, updated_at)
                VALUES (?, 1, ?, ?)
                """, identifier, now, now);
        } else {
            var row = result.rows().get(0);
            int attempts = ((Number) row.get("ATTEMPT_COUNT")).intValue();
            long firstAttempt = ((Number) row.get("FIRST_ATTEMPT_AT")).longValue();
            
            // Reset if window expired
            if (now - firstAttempt > WINDOW_MS) {
                engine.executeSql("""
                    UPDATE auth_rate_limits SET attempt_count = 1, first_attempt_at = ?, updated_at = ?
                    WHERE identifier = ?
                    """, now, now, identifier);
            } else if (attempts >= MAX_ATTEMPTS) {
                // Lock out
                long lockedUntil = now + LOCKOUT_DURATION_MS;
                engine.executeSql("""
                    UPDATE auth_rate_limits SET locked_until = ?, updated_at = ?
                    WHERE identifier = ?
                    """, lockedUntil, now, identifier);
                lockoutCache.put(identifier, lockedUntil);
            } else {
                engine.executeSql("""
                    UPDATE auth_rate_limits SET attempt_count = attempt_count + 1, updated_at = ?
                    WHERE identifier = ?
                    """, now, identifier);
            }
        }
    }
    
    /**
     * Reset attempts (successful login)
     */
    public void resetAttempts(String identifier) {
        engine.executeSql("DELETE FROM auth_rate_limits WHERE identifier = ?", identifier);
        lockoutCache.remove(identifier);
    }
    
    /**
     * Get remaining attempts before lockout
     */
    public int getRemainingAttempts(String identifier) {
        var result = engine.executeSql(
            "SELECT attempt_count FROM auth_rate_limits WHERE identifier = ?",
            identifier
        );
        
        if (!result.success() || result.rows() == null || result.rows().isEmpty()) {
            return MAX_ATTEMPTS;
        }
        
        int attempts = ((Number) result.rows().get(0).get("ATTEMPT_COUNT")).intValue();
        return Math.max(0, MAX_ATTEMPTS - attempts);
    }
    
    /**
     * Get lockout remaining seconds
     */
    public long getLockoutRemainingSeconds(String identifier) {
        Long lockedUntil = lockoutCache.get(identifier);
        if (lockedUntil == null) {
            var result = engine.executeSql(
                "SELECT locked_until FROM auth_rate_limits WHERE identifier = ?",
                identifier
            );
            if (result.success() && result.rows() != null && !result.rows().isEmpty()) {
                lockedUntil = ((Number) result.rows().get(0).get("LOCKED_UNTIL")).longValue();
            }
        }
        
        if (lockedUntil != null && lockedUntil > System.currentTimeMillis()) {
            return (lockedUntil - System.currentTimeMillis()) / 1000;
        }
        
        return 0;
    }
    
    /**
     * Schedule periodic cleanup
     */
    private void scheduleCleanup() {
        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(60 * 60 * 1000);  // Every hour
                } catch (InterruptedException e) {
                    break;
                }
                
                long now = System.currentTimeMillis();
                engine.executeSql("""
                    DELETE FROM auth_rate_limits 
                    WHERE updated_at < ? OR (locked_until IS NOT NULL AND locked_until < ?)
                    """, now - (24 * 60 * 60 * 1000), now);
                lockoutCache.clear();
            }
        }, "RateLimit-Cleanup").start();
    }
}
