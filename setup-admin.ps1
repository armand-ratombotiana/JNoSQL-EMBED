# JunifyDB Admin User Setup Script
# This script creates the admin user for username/password authentication

Write-Host "=== JunifyDB Admin User Setup ===" -ForegroundColor Cyan
Write-Host ""

# Stop any running Java processes
Write-Host "Stopping existing server..." -ForegroundColor Yellow
Get-Process java -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Sleep -Seconds 2

# Generate salt and hash
$salt = [Guid]::NewGuid().ToString().Substring(0, 8)
$password = "admin123"
$bytes = [System.Security.Cryptography.SHA256]::Create().ComputeHash([Text.Encoding]::UTF8.GetBytes($password + $salt))
$hash = [System.BitConverter]::ToString($bytes).Replace("-", "").ToLower()

Write-Host "Salt: $salt" -ForegroundColor Gray
Write-Host "Hash: $hash" -ForegroundColor Gray
Write-Host ""

# Connect to database and create admin user
try {
    Write-Host "Connecting to database..." -ForegroundColor Yellow
    
    # Load H2 driver
    Add-Type -Path "target/dep/h2-2.4.240.jar"
    
    # Connection string
    $dbUrl = "jdbc:h2:file:./data/embeddb;MODE=MySQL"
    $conn = [org.h2.jdbcx.JdbcDataSource]::new()
    $conn.URL = $dbUrl
    $conn.user = "sa"
    $conn.password = "sa"
    $connection = $conn.getConnection()
    
    Write-Host "✓ Connected to database" -ForegroundColor Green
    
    # Create db_users table if not exists
    $createTable = "CREATE TABLE IF NOT EXISTS db_users (username VARCHAR(255) PRIMARY KEY, password_hash VARCHAR(512), salt VARCHAR(128), role VARCHAR(50) DEFAULT 'USER', created_at BIGINT, last_login BIGINT, enabled BOOLEAN DEFAULT TRUE)"
    $connection.createStatement().execute($createTable)
    Write-Host "✓ db_users table created/verified" -ForegroundColor Green
    
    # Check if admin exists
    $checkStmt = $connection.prepareStatement("SELECT COUNT(*) as cnt FROM db_users WHERE username = 'admin'")
    $rs = $checkStmt.executeQuery()
    $rs.next()
    $exists = $rs.getInt("cnt") -gt 0
    
    if ($exists) {
        Write-Host "✓ Admin user exists - resetting password" -ForegroundColor Yellow
        
        $updateStmt = $connection.prepareStatement("UPDATE db_users SET password_hash = ?, salt = ? WHERE username = 'admin'")
        $updateStmt.setString(1, $hash)
        $updateStmt.setString(2, $salt)
        $updateStmt.executeUpdate()
        Write-Host "✓ Password reset to: admin123" -ForegroundColor Green
    } else {
        Write-Host "✓ Creating admin user" -ForegroundColor Yellow
        
        $insertStmt = $connection.prepareStatement("INSERT INTO db_users (username, password_hash, salt, role, created_at, enabled) VALUES (?, ?, ?, ?, ?, ?)")
        $insertStmt.setString(1, "admin")
        $insertStmt.setString(2, $hash)
        $insertStmt.setString(3, $salt)
        $insertStmt.setString(4, "ADMIN")
        $insertStmt.setLong(5, [DateTime]::Now.Ticks)
        $insertStmt.setBoolean(6, $true)
        $insertStmt.executeUpdate()
        Write-Host "✓ Admin user created" -ForegroundColor Green
    }
    
    $connection.close()
    
    Write-Host ""
    Write-Host "===========================================" -ForegroundColor Cyan
    Write-Host "  ✓ ADMIN USER READY" -ForegroundColor Green
    Write-Host "===========================================" -ForegroundColor Cyan
    Write-Host "  Username: admin" -ForegroundColor White
    Write-Host "  Password: admin123" -ForegroundColor White
    Write-Host "  Role: ADMIN" -ForegroundColor White
    Write-Host "===========================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Starting server..." -ForegroundColor Yellow
    
    # Start server
    Start-Process -FilePath "java" -ArgumentList "-cp", "target/classes;target/dep/*", "org.junify.db.JunifyDB", "--port", "8080", "--engine", "H2" -WindowStyle Hidden
    
    Start-Sleep -Seconds 5
    
    Write-Host "✓ Server started on port 8080" -ForegroundColor Green
    Write-Host ""
    Write-Host "Login at: http://localhost:8080/login.html" -ForegroundColor Cyan
    Write-Host "Test page: http://localhost:8080/auth-test.html" -ForegroundColor Cyan
    Write-Host ""
    
} catch {
    Write-Host "Error: $_" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
}
