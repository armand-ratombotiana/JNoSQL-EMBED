# JunifyDB API Test Script
# Tests various features of the embedded NoSQL database

$apiKey = "hYXuECpj4dM28vf3En47ar2KaA1FPMLVNrmAYYSAoFM"
$baseUrl = "http://localhost:8081/api"
$headers = @{"X-API-Key" = $apiKey; "Content-Type" = "application/json"}

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "  JunifyDB API Test Suite" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host ""

# Test 1: Health Check
Write-Host "[TEST 1] Health Check" -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "$baseUrl/health" -Headers $headers -Method Get
    Write-Host "✓ Status: $($response.status)" -ForegroundColor Green
    Write-Host "✓ Engine: $($response.engine)" -ForegroundColor Green
    Write-Host "✓ Version: $($response.version)" -ForegroundColor Green
    Write-Host "✓ Uptime: $($response.uptime)ms" -ForegroundColor Green
} catch {
    Write-Host "✗ Health check failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 2: Document Collection - Insert
Write-Host "[TEST 2] Document Collection - Insert User" -ForegroundColor Yellow
try {
    $user = @{
        name = "Alice Johnson"
        email = "alice@example.com"
        age = 30
        city = "New York"
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$baseUrl/collections/users" -Headers $headers -Method Post -Body $user
    $userId = $response._id
    Write-Host "✓ User created with ID: $userId" -ForegroundColor Green
} catch {
    Write-Host "✗ Insert failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 3: Document Collection - Get All
Write-Host "[TEST 3] Document Collection - Get All Users" -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "$baseUrl/collections/users" -Headers $headers -Method Get
    Write-Host "✓ Retrieved $($response.Count) user(s)" -ForegroundColor Green
    $response | ForEach-Object { Write-Host "  - $($_.name) ($($_.email))" -ForegroundColor Gray }
} catch {
    Write-Host "✗ Get all failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 4: Key-Value Store
Write-Host "[TEST 4] Key-Value Store - Cache Operations" -ForegroundColor Yellow
try {
    $cacheData = @{
        value = "session_data_12345"
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$baseUrl/kv/cache/session:user1" -Headers $headers -Method Put -Body $cacheData
    Write-Host "✓ Cache entry created" -ForegroundColor Green
    
    $response = Invoke-RestMethod -Uri "$baseUrl/kv/cache/session:user1" -Headers $headers -Method Get
    Write-Host "✓ Retrieved value: $($response.value)" -ForegroundColor Green
} catch {
    Write-Host "✗ KV operation failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 5: List Operations
Write-Host "[TEST 5] List Operations - Task Queue" -ForegroundColor Yellow
try {
    $tasks = @{
        values = @("task1", "task2", "task3")
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$baseUrl/kv/lists/tasks/queue1/rpush" -Headers $headers -Method Post -Body $tasks
    Write-Host "✓ Added $($response.length) tasks to queue" -ForegroundColor Green
    
    $response = Invoke-RestMethod -Uri "$baseUrl/kv/lists/tasks/queue1" -Headers $headers -Method Get
    Write-Host "✓ Queue contains: $($response.values -join ', ')" -ForegroundColor Green
} catch {
    Write-Host "✗ List operation failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 6: Set Operations
Write-Host "[TEST 6] Set Operations - Tags" -ForegroundColor Yellow
try {
    $tags = @{
        members = @("java", "database", "nosql")
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$baseUrl/kv/sets/tags/article1/sadd" -Headers $headers -Method Post -Body $tags
    Write-Host "✓ Added $($response.added) tags" -ForegroundColor Green
    
    $response = Invoke-RestMethod -Uri "$baseUrl/kv/sets/tags/article1" -Headers $headers -Method Get
    Write-Host "✓ Tags: $($response.members -join ', ')" -ForegroundColor Green
} catch {
    Write-Host "✗ Set operation failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 7: Hash Operations
Write-Host "[TEST 7] Hash Operations - User Profile" -ForegroundColor Yellow
try {
    $profile = @{
        fields = @{
            name = "Bob Smith"
            email = "bob@example.com"
            age = "35"
        }
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$baseUrl/kv/hashes/profiles/user:bob/hset" -Headers $headers -Method Post -Body $profile
    Write-Host "✓ Profile fields added: $($response.fieldsAdded)" -ForegroundColor Green
    
    $response = Invoke-RestMethod -Uri "$baseUrl/kv/hashes/profiles/user:bob" -Headers $headers -Method Get
    Write-Host "✓ Profile: $($response.fields | ConvertTo-Json -Compress)" -ForegroundColor Green
} catch {
    Write-Host "✗ Hash operation failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 8: Metrics
Write-Host "[TEST 8] Database Metrics" -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "$baseUrl/metrics" -Headers $headers -Method Get
    Write-Host "✓ Total Operations: $($response.totalOperations)" -ForegroundColor Green
    Write-Host "✓ Cache Hit Rate: $([math]::Round($response.cacheHitRate * 100, 2))%" -ForegroundColor Green
} catch {
    Write-Host "✗ Metrics failed: $_" -ForegroundColor Red
}
Write-Host ""

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "  Test Suite Completed!" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan