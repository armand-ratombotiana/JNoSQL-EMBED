package org.junify.db.storage.spi;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junify.db.storage.spi.H2StorageEngine.SqlResult;

public class UserManager {

    public enum Role {
        ADMIN, USER, READONLY
    }
    
    private final H2StorageEngine engine;
    private final Map<String, UserSession> sessions;
    private final Map<String, AtomicInteger> loginAttempts;
    private static final int MAX_LOGIN_ATTEMPTS = 5;
    private static final long LOCKOUT_DURATION_MS = 300000;

    public UserManager(H2StorageEngine engine) {
        this.engine = engine;
        this.sessions = new ConcurrentHashMap<>();
        this.loginAttempts = new ConcurrentHashMap<>();
        initializeSchema();
    }

    private void initializeSchema() {
        engine.executeSql("CREATE TABLE IF NOT EXISTS db_users (" +
            "username VARCHAR(255) PRIMARY KEY, " +
            "password_hash VARCHAR(512) NOT NULL, " +
            "salt VARCHAR(128) NOT NULL, " +
            "role VARCHAR(50) DEFAULT 'USER', " +
            "created_at BIGINT, " +
            "last_login BIGINT, " +
            "enabled BOOLEAN DEFAULT TRUE)"
        );
        engine.executeSql("CREATE TABLE IF NOT EXISTS db_sessions (" +
            "session_id VARCHAR(512) PRIMARY KEY, " +
            "username VARCHAR(255), " +
            "created_at BIGINT, " +
            "expires_at BIGINT)"
        );
    }

    public SqlResult createUser(String username, String password, String role) {
        if (userExists(username)) {
            return new SqlResult(false, null, 0, "User already exists");
        }
        
        String salt = UUID.randomUUID().toString().substring(0, 8);
        String hash = hashPassword(password, salt);
        
        var sql = "INSERT INTO db_users (username, password_hash, salt, role, created_at) VALUES (?, ?, ?, ?, ?)";
        return engine.executeSql(sql, username, hash, salt, role, System.currentTimeMillis());
    }

    private String hashPassword(String password, String salt) {
        try {
            var md = MessageDigest.getInstance("SHA-256");
            var bytes = (password + salt).getBytes();
            var digest = md.digest(bytes);
            var sb = new StringBuilder();
            for (var b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

    public boolean userExists(String username) {
        var result = engine.executeSql(
            "SELECT * FROM db_users WHERE username = ?",
            username
        );
        return result.success() && result.rows() != null && !result.rows().isEmpty();
    }

    public List<String> getUsers() {
        var result = engine.executeSql("SELECT username FROM db_users");
        if (!result.success() || result.rows() == null) {
            return Collections.emptyList();
        }
        return result.rows().stream()
            .map(row -> (String) row.get("USERNAME"))
            .toList();
    }

    /**
     * Drop a user (admin only operation).
     * @param username the username to drop
     * @param requestedBy the user requesting the deletion (must be admin)
     * @return SqlResult of the operation
     */
    public SqlResult dropUser(String username, String requestedBy) {
        requireAdmin(requestedBy);
        engine.executeSql("DELETE FROM db_sessions WHERE username = ?", username);
        return engine.executeSql("DELETE FROM db_users WHERE username = ?", username);
    }
    
    /**
     * Drop a user (legacy method, kept for backward compatibility).
     * @param username the username to drop
     * @return SqlResult of the operation
     * @deprecated Use dropUser(username, requestedBy) for RBAC enforcement
     */
    @Deprecated
    public SqlResult dropUser(String username) {
        return dropUser(username, "sa"); // Default to admin for legacy calls
    }

    public boolean validatePassword(String username, String password) {
        var result = engine.executeSql(
            "SELECT password_hash, salt FROM db_users WHERE username = ? AND enabled = TRUE",
            username
        );
        if (!result.success() || result.rows() == null || result.rows().isEmpty()) {
            return false;
        }
        
        var row = result.rows().get(0);
        var storedHash = (String) row.get("PASSWORD_HASH");
        var salt = (String) row.get("SALT");
        
        return storedHash.equals(hashPassword(password, salt));
    }

    public boolean isLockedOut(String username) {
        var attempts = loginAttempts.get(username);
        if (attempts == null) return false;
        
        if (attempts.get() >= MAX_LOGIN_ATTEMPTS) {
            var lockout = loginAttempts.get(username + "_lockout");
            if (lockout != null) {
                long lockoutTime = lockout.get();
                if (System.currentTimeMillis() - lockoutTime < LOCKOUT_DURATION_MS) {
                    return true;
                }
            }
        }
        return false;
    }

    public int getFailedAttempts(String username) {
        var attempts = loginAttempts.get(username);
        return attempts != null ? attempts.get() : 0;
    }

    public List<UserInfo> getUserInfo() {
        var result = engine.executeSql(
            "SELECT username, role, created_at, last_login, enabled FROM db_users"
        );
        if (!result.success() || result.rows() == null) {
            return Collections.emptyList();
        }
        
        return result.rows().stream()
            .map(row -> new UserInfo(
                (String) row.get("USERNAME"),
                (String) row.get("ROLE"),
                row.get("CREATED_AT") != null ? ((Number) row.get("CREATED_AT")).longValue() : 0L,
                row.get("LAST_LOGIN") != null ? ((Number) row.get("LAST_LOGIN")).longValue() : null,
                row.get("ENABLED") != null && (Boolean) row.get("ENABLED")
            ))
            .toList();
    }

    /**
     * Get the role of a user.
     * @param username the username to check
     * @return the user's role, or Role.USER if not found
     */
    public Role getUserRole(String username) {
        var result = engine.executeSql(
            "SELECT role FROM db_users WHERE username = ?",
            username
        );
        if (!result.success() || result.rows() == null || result.rows().isEmpty()) {
            return Role.USER; // Default to USER if not found
        }
        var row = result.rows().get(0);
        var roleName = (String) row.get("ROLE");
        try {
            return Role.valueOf(roleName != null ? roleName.toUpperCase() : "USER");
        } catch (IllegalArgumentException e) {
            return Role.USER;
        }
    }

    /**
     * Check if user has ADMIN role.
     * @param username the username to check
     * @return true if user is an admin
     */
    public boolean isAdmin(String username) {
        return getUserRole(username) == Role.ADMIN;
    }

    /**
     * Require admin role, throws exception if not admin.
     * @param username the username to check
     * @throws SecurityException if user is not an admin
     */
    public void requireAdmin(String username) {
        if (!isAdmin(username)) {
            throw new SecurityException("User '" + username + "' does not have ADMIN role");
        }
    }

    /**
     * Update user role (admin only operation).
     * @param username the username to update
     * @param newRole the new role
     * @param requestedBy the user requesting the change (must be admin)
     * @return SqlResult of the operation
     */
    public SqlResult updateUserRole(String username, String newRole, String requestedBy) {
        requireAdmin(requestedBy);
        var result = engine.executeSql(
            "UPDATE db_users SET role = ? WHERE username = ?",
            newRole.toUpperCase(), username
        );
        return result;
    }

    public record UserInfo(String username, String role, long createdAt, Long lastLogin, boolean enabled) {}

    private class UserSession {
        final String sessionId;
        final String username;
        final long createdAt;
        final long expiresAt;

        UserSession(String sessionId, String username, long ttlMs) {
            this.sessionId = sessionId;
            this.username = username;
            this.createdAt = System.currentTimeMillis();
            this.expiresAt = createdAt + ttlMs;
        }

        boolean isExpired() {
            return System.currentTimeMillis() > expiresAt;
        }
    }
}