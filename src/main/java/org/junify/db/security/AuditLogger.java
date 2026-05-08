package org.junify.db.security;

import org.junify.db.storage.spi.H2StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Comprehensive Audit Logger
 * 
 * Security event logging with tamper-evident hash chain:
 * - All authentication events logged
 * - Hash chain for tamper detection
 * - SLF4J integration for real-time monitoring
 * - Security alerts for critical events
 */
public class AuditLogger {
    
    private static final Logger logger = LoggerFactory.getLogger(AuditLogger.class);
    private final H2StorageEngine engine;
    private final Map<String, String> ipCache = new ConcurrentHashMap<>();
    
    public enum EventType {
        LOGIN_SUCCESS,
        LOGIN_FAILURE,
        LOGOUT,
        PASSWORD_CHANGE,
        PASSWORD_RESET_REQUEST,
        ACCOUNT_LOCKED,
        ACCOUNT_UNLOCKED,
        SESSION_INVALIDATED,
        CSRF_FAILURE,
        RATE_LIMIT_EXCEEDED,
        USER_CREATED,
        USER_UPDATED,
        USER_DELETED
    }
    
    public AuditLogger(H2StorageEngine engine) {
        this.engine = engine;
        initializeTables();
    }
    
    private void initializeTables() {
        engine.executeSql("""
            CREATE TABLE IF NOT EXISTS auth_audit_log (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                event_type VARCHAR(50) NOT NULL,
                username VARCHAR(255),
                client_ip VARCHAR(45),
                user_agent VARCHAR(500),
                session_id VARCHAR(512),
                success BOOLEAN,
                failure_reason VARCHAR(255),
                metadata VARCHAR(2048),
                created_at BIGINT NOT NULL,
                INDEX idx_event_type (event_type),
                INDEX idx_username (username),
                INDEX idx_created_at (created_at),
                INDEX idx_client_ip (client_ip)
            )
        """);
        
        // Tamper-evident hash chain
        engine.executeSql("""
            CREATE TABLE IF NOT EXISTS auth_audit_hash_chain (
                sequence_num BIGINT PRIMARY KEY,
                event_id BIGINT NOT NULL,
                previous_hash VARCHAR(64),
                current_hash VARCHAR(64) NOT NULL,
                created_at BIGINT NOT NULL
            )
        """);
    }
    
    /**
     * Log an authentication event
     */
    public void log(EventType eventType, String username, String clientIp, 
                    String userAgent, boolean success, String failureReason,
                    Map<String, Object> metadata) {
        
        long timestamp = System.currentTimeMillis();
        String metadataJson = metadata != null ? 
            toJson(metadata) : null;
        
        // Insert audit record
        var result = engine.executeSql("""
            INSERT INTO auth_audit_log 
            (event_type, username, client_ip, user_agent, success, failure_reason, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, eventType.name(), username, clientIp, userAgent, success, 
               failureReason, metadataJson, timestamp);
        
        if (result.success()) {
            long eventId = result.generatedKey();
            updateHashChain(eventId, timestamp);
        }
        
        // Log to SLF4J for immediate visibility
        String logMessage = String.format(
            "[AUTH AUDIT] %s | user=%s | ip=%s | success=%s%s",
            eventType.name(),
            username != null ? username : "unknown",
            clientIp != null ? clientIp : "unknown",
            success,
            failureReason != null ? " | reason=" + failureReason : ""
        );
        
        if (success) {
            logger.info(logMessage);
        } else {
            logger.warn(logMessage);
        }
        
        // Alert on critical events
        if (eventType == EventType.ACCOUNT_LOCKED || 
            eventType == EventType.CSRF_FAILURE ||
            eventType == EventType.RATE_LIMIT_EXCEEDED) {
            logger.error("[SECURITY ALERT] " + logMessage);
        }
    }
    
    /**
     * Update tamper-evident hash chain
     */
    private void updateHashChain(long eventId, long timestamp) {
        // Get last hash
        var result = engine.executeSql(
            "SELECT MAX(sequence_num) as seq, current_hash as hash FROM auth_audit_hash_chain"
        );
        
        long newSeq = 1;
        String previousHash = "GENESIS";
        
        if (result.success() && result.rows() != null && !result.rows().isEmpty()) {
            var row = result.rows().get(0);
            if (row.get("SEQ") != null) {
                newSeq = ((Number) row.get("SEQ")).longValue() + 1;
            }
            if (row.get("HASH") != null) {
                previousHash = (String) row.get("HASH");
            }
        }
        
        // Calculate current hash
        String content = eventId + ":" + timestamp + ":" + previousHash;
        String currentHash = sha256(content);
        
        engine.executeSql("""
            INSERT INTO auth_audit_hash_chain (sequence_num, event_id, previous_hash, current_hash, created_at)
            VALUES (?, ?, ?, ?, ?)
            """, newSeq, eventId, previousHash, currentHash, timestamp);
    }
    
    /**
     * Verify hash chain integrity (tamper detection)
     */
    public boolean verifyHashChain() {
        var result = engine.executeSql("""
            SELECT event_id, previous_hash, current_hash 
            FROM auth_audit_hash_chain 
            ORDER BY sequence_num
        """);
        
        if (!result.success() || result.rows() == null) return false;
        
        String expectedPrevious = "GENESIS";
        
        for (var row : result.rows()) {
            long eventId = ((Number) row.get("EVENT_ID")).longValue();
            String storedPrevious = (String) row.get("PREVIOUS_HASH");
            String storedCurrent = (String) row.get("CURRENT_HASH");
            
            if (!expectedPrevious.equals(storedPrevious)) {
                logger.error("[AUDIT TAMPER DETECTED] Hash chain broken at event {}", eventId);
                return false;
            }
            
            // Get event timestamp
            var eventResult = engine.executeSql(
                "SELECT created_at FROM auth_audit_log WHERE id = ?", eventId
            );
            
            if (eventResult.success() && eventResult.rows() != null && !eventResult.rows().isEmpty()) {
                long timestamp = ((Number) eventResult.rows().get(0).get("CREATED_AT")).longValue();
                String content = eventId + ":" + timestamp + ":" + storedPrevious;
                expectedPrevious = sha256(content);
                
                if (!expectedPrevious.equals(storedCurrent)) {
                    logger.error("[AUDIT TAMPER DETECTED] Hash mismatch at event {}", eventId);
                    return false;
                }
            }
        }
        
        return true;
    }
    
    /**
     * Get recent events for a user
     */
    public List<AuditEvent> getRecentEvents(String username, int limit) {
        var result = engine.executeSql("""
            SELECT * FROM auth_audit_log 
            WHERE username = ? 
            ORDER BY created_at DESC 
            LIMIT ?
        """, username, limit);
        
        if (!result.success() || result.rows() == null) return Collections.emptyList();
        
        return result.rows().stream()
            .map(row -> new AuditEvent(
                ((Number) row.get("ID")).longValue(),
                EventType.valueOf((String) row.get("EVENT_TYPE")),
                (String) row.get("USERNAME"),
                (String) row.get("CLIENT_IP"),
                (Boolean) row.get("SUCCESS"),
                (String) row.get("FAILURE_REASON"),
                ((Number) row.get("CREATED_AT")).longValue()
            ))
            .toList();
    }
    
    /**
     * Get failed login attempts for an IP
     */
    public int getFailedAttemptsForIp(String clientIp, long since) {
        var result = engine.executeSql("""
            SELECT COUNT(*) as cnt FROM auth_audit_log 
            WHERE client_ip = ? AND event_type = 'LOGIN_FAILURE' AND created_at > ?
        """, clientIp, since);
        
        if (!result.success() || result.rows() == null) return 0;
        
        return ((Number) result.rows().get(0).get("CNT")).intValue();
    }
    
    private String sha256(String input) {
        try {
            var md = java.security.MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes());
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }
    
    private String toJson(Map<String, Object> map) {
        try {
            var writer = new java.io.StringWriter();
            writer.write("{");
            boolean first = true;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                if (!first) writer.write(",");
                writer.write("\"").write(entry.getKey()).write("\":\"");
                writer.write(String.valueOf(entry.getValue()));
                writer.write("\"");
                first = false;
            }
            writer.write("}");
            return writer.toString();
        } catch (Exception e) {
            return "{}";
        }
    }
    
    public record AuditEvent(long id, EventType eventType, String username, String clientIp,
                             boolean success, String failureReason, long timestamp) {}
}
