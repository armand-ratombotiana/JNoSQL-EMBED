# JunifyDB Web Console - Comprehensive UI/UX & Security Improvement Plan

**Generated:** May 8, 2026  
**Branch:** `feature/ui-ux-redesign`  
**Status:** Assessment Complete - Ready for Implementation

---

## Executive Summary

This document consolidates findings from three specialized agent assessments covering:
1. **UI/UX Implementation Issues** - 42 issues identified (8 Critical, 19 Major, 15 Minor)
2. **API Integration Gaps** - 57 backend endpoints without frontend, 5 frontend calls to missing endpoints
3. **Authentication Security** - 12 vulnerabilities (4 Critical, 5 High, 3 Medium)

**Total Effort Estimate:** 230+ hours (6-8 weeks with 2-3 developers)

---

## Part 1: Assessment Summary

### 1.1 UI/UX Issues by Severity

| Severity | Count | Priority |
|----------|-------|----------|
| 🔴 Critical | 8 | Fix Immediately |
| 🟠 Major | 19 | High Priority |
| 🟡 Minor | 15 | Medium Priority |
| **Total** | **42** | - |

### 1.2 API Integration Status

| Category | Backend Endpoints | Frontend Integrated | Gaps |
|----------|------------------|---------------------|------|
| Document Collections | 9 | 5 | 4 |
| Key-Value Store | 3 | 3 | 3 |
| Redis-Style (Lists/Sets/Hashes) | 40 | 0 | 40 |
| Column-Family | 14 | 4 | 10 |
| SQL (H2) | 1 | 1 | 4 |
| Authentication | 4 | 3 | 1 |
| Transactions | 3 | 1 | 2 |
| Vectors | 5 | 1 | 4 |
| System Operations | 16 | 10 | 6 |
| **Total** | **95+** | **28** | **67** |

### 1.3 Security Vulnerabilities

| Severity | Count | Status |
|----------|-------|--------|
| 🔴 Critical | 4 | Requires Immediate Fix |
| 🟠 High | 5 | Fix Before Production |
| 🟡 Medium | 3 | Fix in Next Sprint |
| **Total** | **12** | - |

---

## Part 2: Critical Issues (Fix in Week 1)

### C1: Broken String Escaping in HTML

**Files:** `index.html:673, 680`  
**Issue:** Literal `\n` characters appearing in rendered HTML  
**Impact:** Broken visual design, unprofessional appearance  
**Effort:** 30 minutes  

**Fix:**
```html
<!-- BEFORE -->
<link rel="stylesheet" href="css/enhancements.css">\n</head>

<!-- AFTER -->
<link rel="stylesheet" href="css/enhancements.css">
</head>
```

---

### C2: SQL Injection in Password Validation

**Files:** `UserManager.java:115`  
**Issue:** Direct string concatenation with user input  
**Impact:** Complete authentication bypass possible  
**Effort:** 1 hour  

**Fix:**
```java
// BEFORE
var result = engine.executeSql(
    "SELECT password_hash, salt FROM db_users WHERE username = '" + username + "' AND enabled = TRUE"
);

// AFTER
var result = engine.executeSql(
    "SELECT password_hash, salt FROM db_users WHERE username = ? AND enabled = TRUE",
    username
);
```

---

### C3: Hardcoded Default API Key

**Files:** `JunifyDBServer.java:52`, `auth-test.html:92`  
**Issue:** Same API key in all installations, exposed in client code  
**Impact:** Any attacker knows default credentials  
**Effort:** 2 hours  

**Fix:**
```java
// Generate unique key on first startup
private String generateUniqueApiKey() {
    SecureRandom random = new SecureRandom();
    byte[] bytes = new byte[32];
    random.nextBytes(bytes);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
}
```

---

### C4: Missing Backend Endpoints (Frontend Calls)

**Files:** `login.html`, `index.html`  
**Issue:** Frontend calls non-existent endpoints  
**Impact:** Features completely broken  

| Endpoint | Feature | Effort |
|----------|---------|--------|
| `POST /api/auth/change-password` | Password change | 4h |
| `PUT /api/transactions` | Transaction commit | 3h |
| `DELETE /api/transactions` | Transaction rollback | 3h |
| `POST /api/vectors/{index}/search` | Vector search | 6h |

---

### C5: Weak Password Hashing (SHA-256)

**Files:** `AuthenticationHandler.java:284`, `UserManager.java:68`  
**Issue:** SHA-256 is cryptographically broken for passwords  
**Impact:** Passwords can be cracked in minutes  
**Effort:** 4 hours  

**Fix:** Migrate to Argon2id (see Part 4)

---

### C6-C8: Additional Critical Issues

| Issue | File | Impact | Effort |
|-------|------|--------|--------|
| Missing form validation | `login.html`, `index.html` | Poor error discovery | 2h |
| Session expiry uses `alert()` | `index.html:2712` | Broken UX | 1h |
| No error notifications | `index.html:1832` | Silent failures | 2h |

---

## Part 3: Phased Implementation Plan

### Phase 1: Critical Security & Stability (Week 1)

**Goal:** Fix all P0/Critical issues

| Task | Files | Effort | Owner |
|------|-------|--------|-------|
| Fix HTML string escaping | `index.html` | 0.5h | Frontend |
| Fix SQL injection | `UserManager.java` | 1h | Backend |
| Remove hardcoded API key | `JunifyDBServer.java`, `auth-test.html` | 2h | Backend |
| Implement missing endpoints | `JunifyDBServer.java` | 16h | Backend |
| Replace SHA-256 with Argon2id | `AuthenticationHandler.java`, `UserManager.java` | 4h | Backend |
| Add form validation | `login.html`, `index.html` | 2h | Frontend |
| Replace alert() with modal | `index.html` | 1h | Frontend |
| Add error notifications | `index.html` | 2h | Frontend |
| **Total** | - | **28.5h** | - |

**Deliverables:**
- ✅ No critical security vulnerabilities
- ✅ No broken UI elements
- ✅ All frontend calls have backend support

---

### Phase 2: Authentication Redesign (Week 2)

**Goal:** Implement secure, modern authentication system

| Task | Files | Effort | Owner |
|------|-------|--------|-------|
| Secure session cookies | New `SecureSessionManager.java` | 4h | Backend |
| CSRF token protection | New `CsrfTokenManager.java` | 3h | Backend |
| Database-backed rate limiting | New `PersistentRateLimiter.java` | 4h | Backend |
| Password policy enforcement | New `PasswordPolicy.java` | 3h | Backend |
| Comprehensive audit logging | New `AuditLogger.java` | 6h | Backend |
| Modern login page UI | `login.html` | 8h | Frontend |
| Password strength meter | `login.html` | 3h | Frontend |
| Caps lock warning | `login.html` | 1h | Frontend |
| Show/hide password toggle | `login.html` | 1h | Frontend |
| **Total** | - | **33h** | - |

**Deliverables:**
- ✅ OWASP-compliant authentication
- ✅ Modern, professional login UI
- ✅ Comprehensive security audit trail

---

### Phase 3: Accessibility & UX (Week 3)

**Goal:** Fix all Major accessibility and UX issues

| Task | Files | Effort | Owner |
|------|-------|--------|-------|
| Add ARIA labels/roles | `index.html` | 4h | Frontend |
| Implement focus management | `index.html`, `enhancements.js` | 3h | Frontend |
| Add skip navigation link | `index.html`, `login.html` | 1h | Frontend |
| Fix color contrast | `enhancements.css` | 2h | Frontend |
| Add prefers-reduced-motion | `enhancements.css` | 2h | Frontend |
| Fix form label associations | `index.html` | 2h | Frontend |
| Add password reset flow | New `PasswordResetHandler.java`, `reset-password.html` | 6h | Both |
| Expand responsive breakpoints | `enhancements.css` | 3h | Frontend |
| Fix focus state styling | `enhancements.css` | 2h | Frontend |
| Improve toast notifications | `index.html`, `enhancements.js` | 2h | Frontend |
| **Total** | - | **27h** | - |

**Deliverables:**
- ✅ WCAG 2.1 AA compliance
- ✅ Professional UX throughout
- ✅ Mobile-responsive design

---

### Phase 4: API Integration - Core Features (Week 4)

**Goal:** Integrate missing backend features into UI

| Task | Files | Effort | Owner |
|------|-------|--------|-------|
| Document single-view/edit | `index.html` | 4h | Frontend |
| Collection statistics display | `index.html` | 2h | Frontend |
| TTL management UI | `index.html` | 3h | Frontend |
| KV batch operations UI | `index.html` | 4h | Frontend |
| Column-family pagination | `index.html` | 4h | Frontend |
| Column filtering UI | `index.html` | 3h | Frontend |
| Single column editor | `index.html` | 3h | Frontend |
| SQL query timeout config | `index.html` | 2h | Frontend |
| SQL result export to CSV | `index.html` | 2h | Frontend |
| **Total** | - | **27h** | - |

**Deliverables:**
- ✅ Full CRUD for all data models
- ✅ Advanced query features
- ✅ Data export capabilities

---

### Phase 5: Redis-Style Data Structures (Week 5)

**Goal:** Add complete UI for Lists, Sets, and Hashes

| Task | Files | Effort | Owner |
|------|-------|--------|-------|
| Lists tab UI | New `lists.html` or tab in `index.html` | 8h | Frontend |
| List operations (push/pop/trim) | `index.html` | 6h | Frontend |
| List visualization | `index.html` | 4h | Frontend |
| Sets tab UI | New `sets.html` or tab in `index.html` | 8h | Frontend |
| Set operations (add/remove/contains) | `index.html` | 6h | Frontend |
| Set algebra (diff/inter/union) | `index.html` | 4h | Frontend |
| Hashes tab UI | New `hashes.html` or tab in `index.html` | 8h | Frontend |
| Hash field editor | `index.html` | 6h | Frontend |
| Hash increment operations | `index.html` | 3h | Frontend |
| **Total** | - | **53h** | - |

**Deliverables:**
- ✅ Full Redis-style data structure support
- ✅ Visual editors for all types
- ✅ Advanced operations UI

---

### Phase 6: Advanced Features (Week 6)

**Goal:** Implement advanced system features

| Task | Files | Effort | Owner |
|------|-------|--------|-------|
| Audit log viewer UI | New `audit-log.html` | 6h | Frontend |
| CDC management UI | New `cdc.html` | 12h | Both |
| Bulk import/export UI | `index.html` | 6h | Frontend |
| Session management dashboard | New `sessions.html` | 6h | Frontend |
| Vector search UI | `index.html` | 6h | Frontend |
| Vector index management | `index.html` | 4h | Frontend |
| Transaction status list | `index.html` | 3h | Frontend |
| Metrics real-time stream (SSE) | `index.html` | 4h | Frontend |
| User management UI | New `users.html` | 6h | Frontend |
| **Total** | - | **53h** | - |

**Deliverables:**
- ✅ Complete system administration
- ✅ Real-time monitoring
- ✅ User management

---

### Phase 7: Polish & Optimization (Week 7-8)

**Goal:** Final polish, performance optimization, testing

| Task | Files | Effort | Owner |
|------|-------|--------|-------|
| Remove console.log statements | All JS files | 2h | Frontend |
| Add CSP meta tag | `index.html`, `login.html` | 2h | Backend |
| Implement service worker | New `sw.js` | 6h | Frontend |
| Add print stylesheet | `enhancements.css` | 3h | Frontend |
| Performance optimization | All files | 8h | Both |
| Cross-browser testing | All files | 8h | QA |
| Accessibility audit | All files | 4h | QA |
| Security penetration testing | All files | 8h | Security |
| Documentation updates | `README.md`, guides | 8h | Tech Writing |
| **Total** | - | **49h** | - |

**Deliverables:**
- ✅ Production-ready codebase
- ✅ Comprehensive documentation
- ✅ Security audit passed

---

## Part 4: Technical Implementation Details

### 4.1 Argon2id Password Hashing

**Maven Dependency:**
```xml
<dependency>
    <groupId>org.bouncycastle</groupId>
    <artifactId>bcprov-jdk18on</artifactId>
    <version>1.78</version>
</dependency>
```

**Implementation:**
```java
package org.junify.db.security;

import org.bouncycastle.crypto.generators.Argon2BytesGenerator;
import org.bouncycastle.crypto.params.Argon2Parameters;
import java.security.SecureRandom;
import java.util.Base64;

public class PasswordHasher {
    
    private static final SecureRandom RANDOM = new SecureRandom();
    
    // OWASP recommended parameters (2024)
    private static final int MEMORY_KB = 65536;    // 64 MB
    private static final int ITERATIONS = 3;
    private static final int PARALLELISM = 4;
    private static final int SALT_LENGTH = 16;     // 128 bits
    private static final int HASH_LENGTH = 32;     // 256 bits
    
    public static HashedPassword hashPassword(String password) {
        byte[] salt = new byte[SALT_LENGTH];
        RANDOM.nextBytes(salt);
        
        Argon2Parameters.Builder builder = new Argon2Parameters.Builder(
            Argon2Parameters.ARGON2_id)
            .withVersion(Argon2Parameters.ARGON2_VERSION_13)
            .withIterations(ITERATIONS)
            .withMemoryAsKB(MEMORY_KB)
            .withParallelism(PARALLELISM)
            .withSalt(salt);
        
        Argon2BytesGenerator generator = new Argon2BytesGenerator();
        generator.init(builder.build());
        
        byte[] hash = generator.generateBytes(password.toCharArray(), HASH_LENGTH);
        
        return new HashedPassword(
            Base64.getEncoder().encodeToString(hash),
            Base64.getEncoder().encodeToString(salt)
        );
    }
    
    public static boolean verifyPassword(String password, String hash, String salt) {
        byte[] hashBytes = Base64.getDecoder().decode(hash);
        byte[] saltBytes = Base64.getDecoder().decode(salt);
        
        Argon2Parameters.Builder builder = new Argon2Parameters.Builder(
            Argon2Parameters.ARGON2_id)
            .withVersion(Argon2Parameters.ARGON2_VERSION_13)
            .withIterations(ITERATIONS)
            .withMemoryAsKB(MEMORY_KB)
            .withParallelism(PARALLELISM)
            .withSalt(saltBytes);
        
        Argon2BytesGenerator generator = new Argon2BytesGenerator();
        generator.init(builder.build());
        
        byte[] checkHash = generator.generateBytes(password.toCharArray(), HASH_LENGTH);
        
        return java.security.MessageDigest.isEqual(hashBytes, checkHash);
    }
    
    public record HashedPassword(String hash, String salt) {}
}
```

---

### 4.2 Secure Session Cookie Manager

```java
package org.junify.db.console.http;

import com.sun.net.httpserver.HttpExchange;
import java.net.HttpCookie;
import java.security.SecureRandom;
import java.util.Base64;

public class SecureSessionManager {
    
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final int SESSION_ID_BYTES = 32;
    
    // Session configuration
    private static final long SESSION_TTL_MS = 30 * 60 * 1000;  // 30 minutes
    private static final long ABSOLUTE_TTL_MS = 8 * 60 * 60 * 1000;  // 8 hours max
    
    public String generateSessionId() {
        byte[] bytes = new byte[SESSION_ID_BYTES];
        RANDOM.nextBytes(bytes);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }
    
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
    
    public void clearSessionCookie(HttpExchange exchange) {
        String setCookieHeader = "JUNIFY_SESSION=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Strict";
        exchange.getResponseHeaders().add("Set-Cookie", setCookieHeader);
    }
    
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
}
```

---

### 4.3 Password Policy

```java
package org.junify.db.security;

import java.util.*;
import java.util.regex.Pattern;

public class PasswordPolicy {
    
    private static final int MIN_LENGTH = 12;
    private static final int MAX_LENGTH = 128;
    
    public static PasswordValidationResult validate(String password) {
        List<String> violations = new ArrayList<>();
        
        if (password == null || password.isEmpty()) {
            violations.add("Password is required");
            return new PasswordValidationResult(false, violations);
        }
        
        if (password.length() < MIN_LENGTH) {
            violations.add("Password must be at least " + MIN_LENGTH + " characters");
        }
        
        if (password.length() > MAX_LENGTH) {
            violations.add("Password must not exceed " + MAX_LENGTH + " characters");
        }
        
        if (!Pattern.compile("[a-z]").matcher(password).find()) {
            violations.add("Password must contain at least one lowercase letter");
        }
        
        if (!Pattern.compile("[A-Z]").matcher(password).find()) {
            violations.add("Password must contain at least one uppercase letter");
        }
        
        if (!Pattern.compile("[0-9]").matcher(password).find()) {
            violations.add("Password must contain at least one number");
        }
        
        if (!Pattern.compile("[^a-zA-Z0-9]").matcher(password).find()) {
            violations.add("Password must contain at least one special character");
        }
        
        return new PasswordValidationResult(violations.isEmpty(), violations);
    }
    
    public static int calculateStrength(String password) {
        int score = 0;
        
        if (password.length() >= 8) score += 20;
        if (password.length() >= 12) score += 20;
        if (Pattern.compile("[a-z]").matcher(password).find()) score += 15;
        if (Pattern.compile("[A-Z]").matcher(password).find()) score += 15;
        if (Pattern.compile("[0-9]").matcher(password).find()) score += 15;
        if (Pattern.compile("[^a-zA-Z0-9]").matcher(password).find()) score += 15;
        
        return Math.min(score, 100);
    }
    
    public record PasswordValidationResult(boolean valid, List<String> violations) {}
}
```

---

## Part 5: Database Schema Updates

```sql
-- Add new columns for enhanced security
ALTER TABLE db_users ADD COLUMN IF NOT EXISTS needs_rehash BOOLEAN DEFAULT FALSE;
ALTER TABLE db_users ADD COLUMN IF NOT EXISTS mfa_secret VARCHAR(255);
ALTER TABLE db_users ADD COLUMN IF NOT EXISTS mfa_enabled BOOLEAN DEFAULT FALSE;
ALTER TABLE db_users ADD COLUMN IF NOT EXISTS password_changed_at BIGINT;
ALTER TABLE db_users ADD COLUMN IF NOT EXISTS last_failed_attempt BIGINT;
ALTER TABLE db_users ADD COLUMN IF NOT EXISTS failed_attempt_count INT DEFAULT 0;

-- Create rate limiting table
CREATE TABLE IF NOT EXISTS auth_rate_limits (
    identifier VARCHAR(255) PRIMARY KEY,
    attempt_count INT DEFAULT 0,
    first_attempt_at BIGINT,
    locked_until BIGINT,
    updated_at BIGINT
);

-- Create audit log table
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
);

-- Create audit hash chain for tamper detection
CREATE TABLE IF NOT EXISTS auth_audit_hash_chain (
    sequence_num BIGINT PRIMARY KEY,
    event_id BIGINT NOT NULL,
    previous_hash VARCHAR(64),
    current_hash VARCHAR(64) NOT NULL,
    created_at BIGINT NOT NULL
);

-- Create user sessions table
CREATE TABLE IF NOT EXISTS user_sessions (
    session_id VARCHAR(512) PRIMARY KEY,
    username VARCHAR(255) NOT NULL,
    created_at BIGINT NOT NULL,
    expires_at BIGINT NOT NULL,
    last_activity BIGINT,
    client_ip VARCHAR(45),
    user_agent VARCHAR(500),
    csrf_token VARCHAR(512)
);
```

---

## Part 6: Success Metrics

### 6.1 Security Metrics

| Metric | Before | Target | After |
|--------|--------|--------|-------|
| OWASP Compliance | 40% | 100% | - |
| Critical Vulnerabilities | 4 | 0 | - |
| High Vulnerabilities | 5 | 0 | - |
| Password Hash Strength | Weak (SHA-256) | Strong (Argon2id) | - |
| Session Security | Basic | OWASP Compliant | - |

### 6.2 UX Metrics

| Metric | Before | Target | After |
|--------|--------|--------|-------|
| WCAG 2.1 AA Compliance | 60% | 100% | - |
| Critical UI Issues | 8 | 0 | - |
| Major UI Issues | 19 | 0 | - |
| Page Load Time | ~2s | <1s | - |
| Mobile Responsiveness | Partial | Full | - |

### 6.3 Feature Completeness

| Category | Before | Target | After |
|----------|--------|--------|-------|
| API Endpoints Integrated | 28/95 (29%) | 85/95 (90%) | - |
| Redis-Style UI | 0% | 100% | - |
| Advanced Features | 20% | 80% | - |
| System Admin UI | 30% | 90% | - |

---

## Part 7: Risk Mitigation

### Technical Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Argon2id performance issues | Low | Medium | Benchmark with load testing |
| Session migration breaks existing users | Medium | High | Implement gradual migration |
| New UI breaks existing workflows | Low | Medium | Comprehensive testing, feature flags |
| Database schema changes cause issues | Low | High | Backup before migration, rollback plan |

### Schedule Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Scope creep | Medium | Medium | Strict prioritization, phase gating |
| Resource constraints | Medium | High | Cross-train team members |
| Integration complexity | High | Medium | Early integration testing |
| Security review delays | Low | Medium | Engage security team early |

---

## Part 8: Next Steps

### Immediate (This Week)

1. ✅ **Git branch created:** `feature/ui-ux-redesign`
2. ✅ **Assessments completed:** UI/UX, API gaps, Security
3. ⏳ **Review this plan** with stakeholders
4. ⏳ **Prioritize Phase 1 tasks**
5. ⏳ **Assign owners** to critical tasks

### Week 1

- [ ] Fix all Critical security vulnerabilities
- [ ] Fix broken HTML/CSS issues
- [ ] Implement missing backend endpoints
- [ ] Deploy to staging for testing

### Week 2

- [ ] Implement secure authentication system
- [ ] Deploy modern login UI
- [ ] Security audit of new implementation

### Week 3-8

- [ ] Execute Phases 3-7
- [ ] Weekly stakeholder demos
- [ ] Continuous integration and testing

---

## Appendix A: Files Requiring Changes

| File | Issues | Priority | Phase |
|------|--------|----------|-------|
| `src/main/resources/static/index.html` | 28 | Critical | 1, 3, 4, 5, 6 |
| `src/main/resources/static/login.html` | 8 | Critical | 1, 2 |
| `src/main/resources/static/auth-test.html` | 2 | Critical | 1 |
| `src/main/resources/static/css/enhancements.css` | 6 | Major | 1, 3, 7 |
| `src/main/resources/static/js/enhancements.js` | 4 | Major | 1, 3 |
| `src/main/java/org/junify/db/console/http/JunifyDBServer.java` | 5 | Critical | 1, 2 |
| `src/main/java/org/junify/db/console/http/AuthenticationHandler.java` | 4 | Critical | 2 |
| `src/main/java/org/junify/db/storage/spi/UserManager.java` | 2 | Critical | 1, 2 |

**New Files to Create:**
- `src/main/java/org/junify/db/security/PasswordHasher.java`
- `src/main/java/org/junify/db/security/PasswordPolicy.java`
- `src/main/java/org/junify/db/console/http/SecureSessionManager.java`
- `src/main/java/org/junify/db/console/http/CsrfTokenManager.java`
- `src/main/java/org/junify/db/security/PersistentRateLimiter.java`
- `src/main/java/org/junify/db/security/AuditLogger.java`
- `src/main/resources/static/reset-password.html`
- `src/main/resources/static/audit-log.html`
- `src/main/resources/static/sessions.html`
- `src/main/resources/static/users.html`
- `src/main/resources/static/cdc.html`
- `src/main/resources/static/sw.js`

---

**Document Version:** 1.0  
**Last Updated:** May 8, 2026  
**Next Review:** May 15, 2026
