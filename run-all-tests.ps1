# JunifyDB Complete Test Suite Runner
# Tests all endpoints and features

$BASE_URL = "http://localhost:8080/api"
$API_KEY = "hYXuECpj4dM28vf3En47ar2KaA1FPMLVNrmAYYSAoFM"
$HEADERS = @{ "X-API-Key" = $API_KEY; "Content-Type" = "application/json" }
$PASS = 0
$FAIL = 0
$RESULTS = @()

function Test-Feature {
    param([string]$Name, [scriptblock]$Test)
    
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "TEST: $Name" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    
    try {
        & $Test
        Write-Host "✓ PASSED: $Name" -ForegroundColor Green
        $script:PASS++
        $script:RESULTS += [PSCustomObject]@{ Name = $Name; Status = "PASS" }
    }
    catch {
        Write-Host "✗ FAILED: $Name - $_" -ForegroundColor Red
        $script:FAIL++
        $script:RESULTS += [PSCustomObject]@{ Name = $Name; Status = "FAIL" }
    }
}

function Assert-True {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) {
        throw $Message
    }
}

# ============================================
# 1. Health Check
# ============================================
Test-Feature "Health Check Endpoint" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/health" -Headers $HEADERS -Method Get
    Write-Host "Status: $($response.status), Engine: $($response.engine), Version: $($response.version)"
    Assert-True ($response.status -eq "ok" -or $response.ContainsKey("status")) "Health status missing"
}

# ============================================
# 2. Document Collection Operations
# ============================================
Test-Feature "Create Document Collection" {
    $doc = @{
        name = "John Doe"
        email = "john@example.com"
        age = 35
        active = $true
        tags = @("developer", "java", "nosql")
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$BASE_URL/collections/users" -Headers $HEADERS -Method Post -Body $doc
    Write-Host "Created document with ID: $($response.id)"
    $script:userId1 = $response.id
    Assert-True ($response.id -ne $null) "Document ID should not be null"
}

Test-Feature "Get Document by ID" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/collections/users/$script:userId1" -Headers $HEADERS -Method Get
    Write-Host "Retrieved: $($response.name)"
    Assert-True ($response.name -eq "John Doe") "Document name mismatch"
}

Test-Feature "Update Document" {
    $update = @{ name = "John Updated"; age = 36 } | ConvertTo-Json
    $response = Invoke-RestMethod -Uri "$BASE_URL/collections/users/$script:userId1" -Headers $HEADERS -Method Put -Body $update
    Write-Host "Updated: $($response.name)"
    Assert-True ($response.name -eq "John Updated") "Update failed"
}

Test-Feature "Create Multiple Documents" {
    for ($i = 1; $i -le 5; $i++) {
        $doc = @{ name = "User $i"; email = "user$i@example.com"; age = (20 + $i); score = (100 * $i) } | ConvertTo-Json
        Invoke-RestMethod -Uri "$BASE_URL/collections/users" -Headers $HEADERS -Method Post -Body $doc | Out-Null
    }
    Write-Host "✓ Created 5 additional documents"
}

Test-Feature "Query All Documents" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/collections/users" -Headers $HEADERS -Method Get
    Write-Host "Total documents: $($response.Count)"
    Assert-True ($response.Count -ge 6) "Should have at least 6 documents"
}

Test-Feature "Delete Document" {
    Invoke-RestMethod -Uri "$BASE_URL/collections/users/$script:userId1" -Headers $HEADERS -Method Delete | Out-Null
    Write-Host "✓ Deleted document $script:userId1"
}

# ============================================
# 3. Key-Value Store Operations
# ============================================
Test-Feature "KV Store - Put Value" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/cache/user:1001" -Headers $HEADERS -Method Put -Body '{"value":"Alice Smith"}'
    Write-Host "Put response: success=$($response.success)"
    Assert-True ($response.success -eq $true) "KV put should succeed"
}

Test-Feature "KV Store - Get Value" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/cache/user:1001" -Headers $HEADERS -Method Get
    Write-Host "Retrieved value: $($response.value)"
    Assert-True ($response.value -eq "Alice Smith") "KV get value mismatch"
}

Test-Feature "KV Store - Put Multiple Values" {
    $pairs = @(
        @{ key = "session:abc"; value = "session-data-123" },
        @{ key = "token:xyz"; value = "bearer-token-456" },
        @{ key = "config:theme"; value = "dark" }
    )
    foreach ($pair in $pairs) {
        $body = @{ value = $pair.value } | ConvertTo-Json
        Invoke-RestMethod -Uri "$BASE_URL/kv/cache/$($pair.key)" -Headers $HEADERS -Method Put -Body $body | Out-Null
    }
    Write-Host "✓ Stored 3 KV pairs"
}

Test-Feature "KV Store - Delete Value" {
    Invoke-RestMethod -Uri "$BASE_URL/kv/cache/config:theme" -Headers $HEADERS -Method Delete | Out-Null
    Write-Host "✓ Deleted KV entry"
}

# ============================================
# 4. Column-Family Operations
# ============================================
Test-Feature "Column-Family - Put Column" {
    $column = @{ family = "profile"; qualifier = "name"; value = "Jane Doe"; timestamp = (Get-Date -Format "yyyy-MM-ddTHH:mm:ss") } | ConvertTo-Json
    $response = Invoke-RestMethod -Uri "$BASE_URL/columns/user_profiles/user123" -Headers $HEADERS -Method Put -Body $column
    Write-Host "Column put response: success=$($response.success)"
}

Test-Feature "Column-Family - Get Column" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/columns/user_profiles/user123" -Headers $HEADERS -Method Get
    Write-Host "Column get response: $($response.Count) columns"
}

Test-Feature "Column-Family - Put Multiple Columns" {
    $columns = @(
        @{ family = "profile"; qualifier = "email"; value = "jane@example.com" },
        @{ family = "profile"; qualifier = "age"; value = "28" },
        @{ family = "settings"; qualifier = "notifications"; value = "enabled" }
    )
    foreach ($col in $columns) {
        $body = $col | ConvertTo-Json
        Invoke-RestMethod -Uri "$BASE_URL/columns/user_profiles/user123" -Headers $HEADERS -Method Put -Body $body | Out-Null
    }
    Write-Host "✓ Stored multiple columns"
}

# ============================================
# 5. Bulk Operations
# ============================================
Test-Feature "Bulk Insert Documents" {
    $docs = @(
        @{ name = "Bulk User 1"; email = "bulk1@test.com"; age = 25 },
        @{ name = "Bulk User 2"; email = "bulk2@test.com"; age = 30 },
        @{ name = "Bulk User 3"; email = "bulk3@test.com"; age = 35 },
        @{ name = "Bulk User 4"; email = "bulk4@test.com"; age = 40 },
        @{ name = "Bulk User 5"; email = "bulk5@test.com"; age = 45 }
    )
    $body = @{ documents = $docs } | ConvertTo-Json -Depth 10
    $response = Invoke-RestMethod -Uri "$BASE_URL/bulk/products" -Headers $HEADERS -Method Post -Body $body
    Write-Host "Bulk insert response: inserted=$($response.inserted)"
    Assert-True ($response.inserted -ge 5) "Bulk insert should insert at least 5 docs"
}

# ============================================
# 6. SQL Operations (H2 Engine)
# ============================================
Test-Feature "SQL - Create Table" {
    $sql = "CREATE TABLE IF NOT EXISTS employees (id INT PRIMARY KEY, name VARCHAR(100), department VARCHAR(50), salary DECIMAL(10,2))"
    $response = Invoke-RestMethod -Uri "$BASE_URL/sql" -Headers $HEADERS -Method Post -Body (@{ sql = $sql } | ConvertTo-Json)
    Write-Host "Create table response: success=$($response.success)"
}

Test-Feature "SQL - Insert Records" {
    $inserts = @(
        "INSERT INTO employees VALUES (1, 'Alice Johnson', 'Engineering', 85000.00)",
        "INSERT INTO employees VALUES (2, 'Bob Smith', 'Marketing', 65000.00)",
        "INSERT INTO employees VALUES (3, 'Carol White', 'Engineering', 92000.00)",
        "INSERT INTO employees VALUES (4, 'David Brown', 'Sales', 72000.00)",
        "INSERT INTO employees VALUES (5, 'Eve Davis', 'Engineering', 88000.00)"
    )
    foreach ($sql in $inserts) {
        Invoke-RestMethod -Uri "$BASE_URL/sql" -Headers $HEADERS -Method Post -Body (@{ sql = $sql } | ConvertTo-Json) | Out-Null
    }
    Write-Host "✓ Inserted 5 employee records"
}

Test-Feature "SQL - Select All" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/sql" -Headers $HEADERS -Method Post -Body (@{ sql = "SELECT * FROM employees" } | ConvertTo-Json)
    Write-Host "Select all: $($response.rows.Count) rows"
    Assert-True ($response.rows.Count -ge 5) "Should have 5 rows"
}

Test-Feature "SQL - Select with WHERE" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/sql" -Headers $HEADERS -Method Post -Body (@{ sql = "SELECT * FROM employees WHERE department = 'Engineering'" } | ConvertTo-Json)
    Write-Host "Engineering dept: $($response.rows.Count) rows"
    Assert-True ($response.rows.Count -ge 3) "Should have 3 engineers"
}

Test-Feature "SQL - Aggregate Query" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/sql" -Headers $HEADERS -Method Post -Body (@{ sql = "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department" } | ConvertTo-Json)
    Write-Host "Aggregation: $($response.rows.Count) groups"
}

Test-Feature "SQL - Update Records" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/sql" -Headers $HEADERS -Method Post -Body (@{ sql = "UPDATE employees SET salary = salary * 1.1 WHERE department = 'Engineering'" } | ConvertTo-Json)
    Write-Host "Update response: rows=$($response.rowsAffected)"
}

Test-Feature "SQL - Delete Records" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/sql" -Headers $HEADERS -Method Post -Body (@{ sql = "DELETE FROM employees WHERE id = 5" } | ConvertTo-Json)
    Write-Host "Delete response: rows=$($response.rowsAffected)"
}

# ============================================
# 7. Schema Operations
# ============================================
Test-Feature "Schema - List Tables" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/schema" -Headers $HEADERS -Method Get
    Write-Host "Schema tables: $($response.Count)"
}

# ============================================
# 8. CDC Operations
# ============================================
Test-Feature "CDC - Enable CDC" {
    $config = @{ enabled = $true; targetType = "MEMORY"; collections = @("users", "products") } | ConvertTo-Json
    $response = Invoke-RestMethod -Uri "$BASE_URL/cdc" -Headers $HEADERS -Method Post -Body $config
    Write-Host "CDC enable response: enabled=$($response.enabled)"
}

Test-Feature "CDC - Get CDC Status" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/cdc" -Headers $HEADERS -Method Get
    Write-Host "CDC status: enabled=$($response.enabled)"
}

# ============================================
# 9. Redis-Style: List Operations
# ============================================
Test-Feature "List - LPUSH" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/mylist/lpush" -Headers $HEADERS -Method Post -Body '{"values": ["c", "b", "a"]}'
    Write-Host "LPUSH response: length=$($response.length)"
}

Test-Feature "List - RPUSH" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/mylist/rpush" -Headers $HEADERS -Method Post -Body '{"values": ["d", "e", "f"]}'
    Write-Host "RPUSH response: length=$($response.length)"
}

Test-Feature "List - LLEN" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/mylist/len" -Headers $HEADERS -Method Get
    Write-Host "LLEN response: length=$($response.length)"
}

Test-Feature "List - LRANGE" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/mylist/range?start=0&end=-1" -Headers $HEADERS -Method Get
    Write-Host "LRANGE response: $($response.elements.Count) elements"
}

Test-Feature "List - LPOP" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/mylist/lpop" -Headers $HEADERS -Method Post -Body '{}'
    Write-Host "LPOP response: value=$($response.value)"
}

Test-Feature "List - RPOP" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/mylist/rpop" -Headers $HEADERS -Method Post -Body '{}'
    Write-Host "RPOP response: value=$($response.value)"
}

# ============================================
# 10. Redis-Style: Set Operations
# ============================================
Test-Feature "Set - SADD" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/sets/demo/myset/sadd" -Headers $HEADERS -Method Post -Body '{"members": ["apple", "banana", "cherry", "date"]}'
    Write-Host "SADD response: added=$($response.added)"
}

Test-Feature "Set - SCARD" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/sets/demo/myset/scard" -Headers $HEADERS -Method Get
    Write-Host "SCARD response: cardinality=$($response.cardinality)"
}

Test-Feature "Set - SMEMBERS" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/sets/demo/myset/smembers" -Headers $HEADERS -Method Get
    Write-Host "SMEMBERS response: $($response.members.Count) members"
}

Test-Feature "Set - SISMEMBER" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/sets/demo/myset/sismember?member=banana" -Headers $HEADERS -Method Get
    Write-Host "SISMEMBER (banana): exists=$($response.exists)"
}

Test-Feature "Set - SPOP" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/sets/demo/myset/spop" -Headers $HEADERS -Method Post -Body '{}'
    Write-Host "SPOP response: popped=$($response.popped)"
}

# ============================================
# 11. Redis-Style: Hash Operations
# ============================================
Test-Feature "Hash - HSET" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/hset" -Headers $HEADERS -Method Post -Body '{"field": "name", "value": "John Doe"}'
    Write-Host "HSET response: fieldsSet=$($response.fieldsSet)"
}

Test-Feature "Hash - HSET Multiple" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/hset" -Headers $HEADERS -Method Post -Body '{"fields": {"age": "30", "email": "john@example.com", "city": "New York"}}'
    Write-Host "HSET multiple response: fieldsSet=$($response.fieldsSet)"
}

Test-Feature "Hash - HGET" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/hget?field=name" -Headers $HEADERS -Method Get
    Write-Host "HGET response: value=$($response.value)"
}

Test-Feature "Hash - HGETALL" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/getall" -Headers $HEADERS -Method Get
    Write-Host "HGETALL response: $($response.fields.Count) fields"
}

Test-Feature "Hash - HLEN" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/len" -Headers $HEADERS -Method Get
    Write-Host "HLEN response: length=$($response.length)"
}

Test-Feature "Hash - HEXISTS" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/exists?field=email" -Headers $HEADERS -Method Get
    Write-Host "HEXISTS (email): exists=$($response.exists)"
}

Test-Feature "Hash - HKEYS" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/keys" -Headers $HEADERS -Method Get
    Write-Host "HKEYS response: $($response.keys.Count) keys"
}

Test-Feature "Hash - HVALS" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/vals" -Headers $HEADERS -Method Get
    Write-Host "HVALS response: $($response.values.Count) values"
}

Test-Feature "Hash - HMGET" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/mget" -Headers $HEADERS -Method Post -Body '{"fields": ["name", "email", "city"]}'
    Write-Host "HMGET response: $($response.values.Count) values"
}

Test-Feature "Hash - HINCRBY" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/incrby" -Headers $HEADERS -Method Post -Body '{"field": "loginCount", "delta": 5}'
    Write-Host "HINCRBY response: newValue=$($response.newValue)"
}

Test-Feature "Hash - HDEL" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/del" -Headers $HEADERS -Method Post -Body '{"fields": ["city"]}'
    Write-Host "HDEL response: deleted=$($response.deleted)"
}

# ============================================
# Summary
# ============================================
Write-Host "`n========================================" -ForegroundColor Yellow
Write-Host "TEST SUITE COMPLETE" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow
Write-Host "Passed: $PASS" -ForegroundColor Green
Write-Host "Failed: $FAIL" -ForegroundColor $(if ($FAIL -eq 0) { "Green" } else { "Red" })
Write-Host "Total:  $($PASS + $FAIL)" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow

if ($FAIL -eq 0) {
    Write-Host "✓ ALL TESTS PASSED!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "✗ Some tests failed" -ForegroundColor Red
    exit 1
}
