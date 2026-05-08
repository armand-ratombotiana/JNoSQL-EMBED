package org.junify.db.console.http;

import com.sun.net.httpserver.HttpExchange;

import java.net.HttpCookie;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * Secure Session Manager
 * 
 * Implements OWASP session management best practices:
 * - Secure random session ID generation (256-bit)
 * - HttpOnly cookies (XSS protection)
 * - Secure flag (HTTPS only)
 * - SameSite=Strict (CSRF protection)
 * - Configurable TTL with absolute maximum
 */
public class SecureSessionManager {
    
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int SESSION_ID_BYTES = 32;  // 256 bits
    
    // Session configuration
    private static final long SESSION_TTL_MS = 30 * 60 * 1000;   // 30 minutes
    private static final long ABSOLUTE_TTL_MS = 8 * 60 * 60 * 1000;  // 8 hours max
    private static final int MAX_SESSIONS_PER_USER = 5;
    
    /**
     * Generate a cryptographically secure session ID
     */
    public String generateSessionId() {
        byte[] bytes = new byte[SESSION_ID_BYTES];
        RANDOM.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
    
    /**
     * Set secure session cookie
     */
    public void setSessionCookie(HttpExchange exchange, String sessionId, boolean secure) {
        HttpCookie cookie = new HttpCookie("JUNIFY_SESSION", sessionId);
        cookie.setHttpOnly(true);  // Prevent XSS access
        cookie.setSecure(secure);  // Only over HTTPS
        cookie.setPath("/");
        cookie.setMaxAge(SESSION_TTL_MS / 1000);
        
        // SameSite=Strict for maximum CSRF protection
        String setCookieHeader = String.format(
            "%s; Path=/; Max-Age=%d; HttpOnly; Secure; SameSite=Strict",
            cookie.toString(),
            SESSION_TTL_MS / 1000
        );
        
        exchange.getResponseHeaders().add("Set-Cookie", setCookieHeader);
    }
    
    /**
     * Clear session cookie (logout)
     */
    public void clearSessionCookie(HttpExchange exchange) {
        String setCookieHeader = "JUNIFY_SESSION=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Strict";
        exchange.getResponseHeaders().add("Set-Cookie", setCookieHeader);
    }
    
    /**
     * Get session ID from cookie
     */
    public String getSessionIdFromCookie(HttpExchange exchange) {
        var cookies = exchange.getRequestHeaders().get("Cookie");
        if (cookies == null) return null;
        
        for (String cookieHeader : cookies) {
            for (String cookie : cookieHeader.split(";")) {
                String[] parts = cookie.trim().split("=", 2);
                if (parts.length == 2 && "JUNIFY_SESSION".equals(parts[0])) {
                    return parts[1];
                }
            }
        }
        return null;
    }
    
    /**
     * Get session TTL in milliseconds
     */
    public long getSessionTtlMs() {
        return SESSION_TTL_MS;
    }
    
    /**
     * Get absolute session TTL
     */
    public long getAbsoluteTtlMs() {
        return ABSOLUTE_TTL_MS;
    }
    
    /**
     * Get max sessions per user
     */
    public int getMaxSessionsPerUser() {
        return MAX_SESSIONS_PER_USER;
    }
}
