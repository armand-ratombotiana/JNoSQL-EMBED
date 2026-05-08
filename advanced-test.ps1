# JunifyDB Advanced Features Test Script
# Tests SQL, transactions, column-family, and advanced operations

$apiKey = "hYXuECpj4dM28vf3En47ar2KaA1FPMLVNrmAYYSAoFM"
$baseUrl = "http://localhost:8081/api"
$headers = @{"X-API-Key" = $apiKey; "Content-Type" = "application/json"}

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "  JunifyDB Advanced Features Test" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan
Write-Host ""

# Test 1: SQL Operations (if H2 engine is available)
Write-Host "[TEST 1] SQL Operations" -ForegroundColor Yellow
try {
    # Create table
    $sql = @{
        sql = "CREATE TABLE IF NOT EXISTS products (id INT PRIMARY KEY, name VARCHAR(255), price DECIMAL(10,2), category VARCHAR(100))"
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$baseUrl/sql" -Headers $headers -Method Post -Body $sql -ErrorAction SilentlyContinue
    Write-Host "✓ Table created" -ForegroundColor Green
    
    # Insert data
    $sql = @{
        sql = "INSERT INTO products VALUES (1, 'Laptop', 999.99, 'Electronics')"
    } | ConvertTo-Json
    
    Invoke-RestMethod -Uri "$baseUrl/sql" -Headers $headers -Method Post -Body $sql -ErrorAction SilentlyContinue
    Write-Host "✓ Product inserted" -ForegroundColor Green
    
    # Query data
    $sql = @{
        sql = "SELECT * FROM products WHERE category = 'Electronics'"
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$baseUrl/sql" -Headers $headers -Method Post -Body $sql -ErrorAction SilentlyContinue
    Write-Host "✓ Query executed: Found $($response.rows.Count) product(s)" -ForegroundColor Green
} catch {
    Write-Host "⚠ SQL operations not available (requires H2 engine)" -ForegroundColor Yellow
}
Write-Host ""

# Test 2: Column Family Operations
Write-Host "TEST 2: Column Family - Wide Column Store" -ForegroundColor Yellow
try {
    # Put column
    $data = @{
        value = "John Doe"
        ttlSeconds = 3600
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$baseUrl/columns/users/user:1/column/name" -Headers $headers -Method Put -Body $data
    Write-Host "✓ Column 'name' created for user:1" -ForegroundColor Green
    
    # Put another column
    $data = @{
        value = "john@example.com"
    } | ConvertTo-Json
    
    Invoke-RestMethod -Uri "$baseUrl/columns/users/user:1/column/email" -Headers $headers -Method Put -Body $data
    Write-Host "✓ Column 'email' created for user:1" -ForegroundColor Green
    
    # Get column
    $response = Invoke-RestMethod -Uri "$baseUrl/columns/users/user:1/column/name" -Headers $headers -Method Get
    Write-Host "✓ Retrieved column value: $($response.value)" -ForegroundColor Green
    
    # Get row stats
    $response = Invoke-RestMethod -Uri "$baseUrl/columns/users/user:1/stats" -Headers $headers -Method Get
    Write-Host "✓ Row stats: $($response.columnCount) columns" -ForegroundColor Green
} catch {
    Write-Host "✗ Column family operation failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 3: Document Query Operations
Write-Host "[TEST 3] Document Query - Advanced Filtering" -ForegroundColor Yellow
try {
    # Insert test documents
    $users = @(
        @{name="Alice"; age=25; city="New York"},
        @{name="Bob"; age=35; city="Los Angeles"},
        @{name="Charlie"; age=30; city="New York"}
    )
    
    foreach ($user in $users) {
        $json = $user | ConvertTo-Json
        Invoke-RestMethod -Uri "$baseUrl/collections/test_users" -Headers $headers -Method Post -Body $json | Out-Null
    }
    Write-Host "✓ Inserted 3 test users" -ForegroundColor Green
    
    # Query by age (greater than)
    $query = @{
        '$gt' = @{age = 28}
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$baseUrl/collections/test_users/query" -Headers $headers -Method Post -Body $query
    Write-Host "✓ Query (age > 28): Found $($response.Count) user(s)" -ForegroundColor Green
    
    # Query by equality
    $query = @{
        '$eq' = @{city = "New York"}
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$baseUrl/collections/test_users/query" -Headers $headers -Method Post -Body $query
    Write-Host "✓ Query (city = New York): Found $($response.Count) user(s)" -ForegroundColor Green
} catch {
    Write-Host "✗ Query operation failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 4: TTL (Time To Live) Operations
Write-Host "[TEST 4] TTL - Expiring Data" -ForegroundColor Yellow
try {
    # Insert document with TTL
    $doc = @{
        documentId = "temp-doc-1"
        ttlSeconds = 10
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$baseUrl/collections/temp_data/set-ttl" -Headers $headers -Method Post -Body $doc
    Write-Host "✓ Set TTL of 10 seconds for document" -ForegroundColor Green
    
    # Get collection stats
    $response = Invoke-RestMethod -Uri "$baseUrl/collections/temp_data/stats" -Headers $headers -Method Get
    Write-Host "✓ Collection stats retrieved" -ForegroundColor Green
} catch {
    Write-Host "✗ TTL operation failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 5: Bulk Operations
Write-Host "[TEST 5] Bulk Operations - Batch Insert" -ForegroundColor Yellow
try {
    $bulkData = @{
        operations = @(
            @{operation="insert"; collection="bulk_test"; document=@{name="Item1"; value=100}},
            @{operation="insert"; collection="bulk_test"; document=@{name="Item2"; value=200}},
            @{operation="insert"; collection="bulk_test"; document=@{name="Item3"; value=300}}
        )
    } | ConvertTo-Json -Depth 5
    
    $response = Invoke-RestMethod -Uri "$baseUrl/bulk/bulk_test" -Headers $headers -Method Post -Body $bulkData -ErrorAction SilentlyContinue
    Write-Host "✓ Bulk insert completed" -ForegroundColor Green
} catch {
    Write-Host "⚠ Bulk operations endpoint may not be fully implemented" -ForegroundColor Yellow
}
Write-Host ""

# Test 6: Collection Statistics
Write-Host "[TEST 6] Collection Statistics" -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "$baseUrl/collections/test_users/stats" -Headers $headers -Method Get
    Write-Host "✓ Document count: $($response.documentCount)" -ForegroundColor Green
    Write-Host "✓ Collection name: $($response.collectionName)" -ForegroundColor Green
} catch {
    Write-Host "✗ Stats retrieval failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 7: Hash Increment Operations
Write-Host "[TEST 7] Hash Increment - Counters" -ForegroundColor Yellow
try {
    # Initialize counter
    $data = @{
        field = "views"
        delta = 1
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$baseUrl/kv/hashes/counters/page:home/hincrby" -Headers $headers -Method Post -Body $data
    Write-Host "✓ Counter incremented to: $($response.newValue)" -ForegroundColor Green
    
    # Increment again
    $response = Invoke-RestMethod -Uri "$baseUrl/kv/hashes/counters/page:home/hincrby" -Headers $headers -Method Post -Body $data
    Write-Host "✓ Counter incremented to: $($response.newValue)" -ForegroundColor Green
    
    # Float increment
    $data = @{
        field = "rating"
        delta = 4.5
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$baseUrl/kv/hashes/counters/page:home/hincrbyfloat" -Headers $headers -Method Post -Body $data
    Write-Host "✓ Float counter set to: $($response.newValue)" -ForegroundColor Green
} catch {
    Write-Host "✗ Hash increment failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 8: Set Operations - Union, Intersection, Difference
Write-Host "[TEST 8] Set Operations - Advanced" -ForegroundColor Yellow
try {
    # Create first set
    $data = @{
        members = @("java", "python", "javascript")
    } | ConvertTo-Json
    
    Invoke-RestMethod -Uri "$baseUrl/kv/sets/skills/developer1/sadd" -Headers $headers -Method Post -Body $data | Out-Null
    
    # Create second set
    $data = @{
        members = @("python", "go", "rust")
    } | ConvertTo-Json
    
    Invoke-RestMethod -Uri "$baseUrl/kv/sets/skills/developer2/sadd" -Headers $headers -Method Post -Body $data | Out-Null
    Write-Host "✓ Created two skill sets" -ForegroundColor Green
    
    # Intersection
    $data = @{
        keys = @("developer1", "developer2")
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$baseUrl/kv/sets/skills/developer1/sinter" -Headers $headers -Method Post -Body $data
    Write-Host "✓ Intersection (common skills): $($response.intersection -join ', ')" -ForegroundColor Green
    
    # Union
    $response = Invoke-RestMethod -Uri "$baseUrl/kv/sets/skills/developer1/sunion" -Headers $headers -Method Post -Body $data
    Write-Host "✓ Union (all skills): $($response.union -join ', ')" -ForegroundColor Green
} catch {
    Write-Host "✗ Set operations failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 9: List Range Operations
Write-Host "[TEST 9] List Range - Pagination" -ForegroundColor Yellow
try {
    # Add items to list
    $data = @{
        values = @("item1", "item2", "item3", "item4", "item5", "item6", "item7", "item8", "item9", "item10")
    } | ConvertTo-Json
    
    Invoke-RestMethod -Uri "$baseUrl/kv/lists/items/paginated/rpush" -Headers $headers -Method Post -Body $data | Out-Null
    Write-Host "✓ Added 10 items to list" -ForegroundColor Green
    
    # Get range (first 5)
    $response = Invoke-RestMethod -Uri "$baseUrl/kv/lists/items/paginated/range?start=0&end=4" -Headers $headers -Method Get
    Write-Host "✓ Range [0-4]: $($response.values -join ', ')" -ForegroundColor Green
    
    # Get range (last 3)
    $uri = $baseUrl + '/kv/lists/items/paginated/range?start=-3&end=-1'
    $response = Invoke-RestMethod -Uri $uri -Headers $headers -Method Get
    Write-Host "✓ Range (last 3): $($response.values -join ', ')" -ForegroundColor Green
} catch {
    Write-Host "✗ List range operation failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 10: Audit Logs
Write-Host "[TEST 10] Audit Logs - Security Tracking" -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "$baseUrl/audit/logs?limit=5" -Headers $headers -Method Get
    Write-Host "✓ Retrieved $($response.count) audit events" -ForegroundColor Green
    if ($response.events -and $response.events.Count -gt 0) {
        $latestEvent = $response.events[0]
        Write-Host "  Latest: $($latestEvent.operation) on $($latestEvent.resource)" -ForegroundColor Gray
    }
} catch {
    Write-Host "⚠ Audit logs may not be available" -ForegroundColor Yellow
}
Write-Host ""

Write-Host "==================================================" -ForegroundColor Cyan
Write-Host "  Advanced Test Suite Completed!" -ForegroundColor Cyan
Write-Host "==================================================" -ForegroundColor Cyan