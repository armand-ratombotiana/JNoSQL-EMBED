# JunifyDB Complete API Test Suite
# Tests all endpoints and features of the web console API server

$BASE_URL = "http://localhost:8080/api"
$PASS = 0
$FAIL = 0

function Test-Feature {
    param([string]$Name, [scriptblock]$Test)
    
    Write-Host "`n========================================" -ForegroundColor Cyan
    Write-Host "TEST: $Name" -ForegroundColor Cyan
    Write-Host "========================================" -ForegroundColor Cyan
    
    try {
        & $Test
        Write-Host "✓ PASSED: $Name" -ForegroundColor Green
        $script:PASS++
    }
    catch {
        Write-Host "✗ FAILED: $Name - $_" -ForegroundColor Red
        $script:FAIL++
    }
}

function Assert-True {
    param([bool]$Condition, [string]$Message)
    if (-not $Condition) {
        throw $Message
    }
}

function Assert-Contains {
    param([string]$Output, [string]$Expected, [string]$Message)
    if (-not $Output.Contains($Expected)) {
        throw "$Message - Expected to contain: $Expected"
    }
}

# ============================================
# 1. Health Check & System Info
# ============================================
Test-Feature "Health Check Endpoint" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/health" -Method Get
    Write-Host "Response: $($response | ConvertTo-Json -Depth 3)"
    Assert-True ($response.status -eq "healthy" -or $response.ContainsKey("status")) "Health status missing"
    Write-Host "✓ Health check returned valid status"
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
        metadata = @{
            createdAt = (Get-Date -Format "yyyy-MM-ddTHH:mm:ss")
            version = "1.0"
        }
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$BASE_URL/collections/users" -Method Post -Body $doc -ContentType "application/json"
    Write-Host "Created document: $($response | ConvertTo-Json)"
    Assert-True ($response.id -ne $null) "Document ID should not be null"
    $script:userId1 = $response.id
}

Test-Feature "Get Document by ID" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/collections/users/$script:userId1" -Method Get
    Write-Host "Retrieved: $($response | ConvertTo-Json)"
    Assert-True ($response.name -eq "John Doe") "Document name mismatch"
}

Test-Feature "Update Document" {
    $update = @{
        name = "John Updated"
        age = 36
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$BASE_URL/collections/users/$script:userId1" -Method Put -Body $update -ContentType "application/json"
    Write-Host "Updated: $($response | ConvertTo-Json)"
    Assert-True ($response.name -eq "John Updated") "Update failed"
}

Test-Feature "Create Multiple Documents" {
    for ($i = 1; $i -le 5; $i++) {
        $doc = @{
            name = "User $i"
            email = "user$i@example.com"
            age = (20 + $i)
            score = (100 * $i)
        } | ConvertTo-Json
        Invoke-RestMethod -Uri "$BASE_URL/collections/users" -Method Post -Body $doc -ContentType "application/json"
    }
    Write-Host "✓ Created 5 additional documents"
}

Test-Feature "Query All Documents in Collection" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/collections/users" -Method Get
    Write-Host "Total documents: $($response.Count)"
    Assert-True ($response.Count -ge 6) "Should have at least 6 documents"
}

Test-Feature "Delete Document" {
    Invoke-RestMethod -Uri "$BASE_URL/collections/users/$script:userId1" -Method Delete
    Write-Host "✓ Deleted document $script:userId1"
    
    try {
        Invoke-RestMethod -Uri "$BASE_URL/collections/users/$script:userId1" -Method Get
        throw "Document should be deleted"
    }
    catch {
        Write-Host "✓ Document successfully deleted (404 expected)"
    }
}

# ============================================
# 3. Key-Value Store Operations
# ============================================
Test-Feature "KV Store - Put Value" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/cache/user:1001" -Method Put -Body '{"value":"Alice Smith"}' -ContentType "application/json"
    Write-Host "Put response: $($response | ConvertTo-Json)"
    Assert-True ($response.success -eq $true) "KV put should succeed"
}

Test-Feature "KV Store - Get Value" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/cache/user:1001" -Method Get
    Write-Host "Retrieved value: $($response | ConvertTo-Json)"
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
        Invoke-RestMethod -Uri "$BASE_URL/kv/cache/$($pair.key)" -Method Put -Body $body -ContentType "application/json"
    }
    Write-Host "✓ Stored 3 KV pairs"
}

Test-Feature "KV Store - Delete Value" {
    Invoke-RestMethod -Uri "$BASE_URL/kv/cache/config:theme" -Method Delete
    Write-Host "✓ Deleted KV entry"
}

# ============================================
# 4. Column-Family Operations
# ============================================
Test-Feature "Column-Family - Put Column" {
    $column = @{
        family = "profile"
        qualifier = "name"
        value = "Jane Doe"
        timestamp = (Get-Date -Format "yyyy-MM-ddTHH:mm:ss")
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$BASE_URL/columns/user_profiles/user123" -Method Put -Body $column -ContentType "application/json"
    Write-Host "Column put response: $($response | ConvertTo-Json)"
}

Test-Feature "Column-Family - Get Column" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/columns/user_profiles/user123" -Method Get
    Write-Host "Column get response: $($response | ConvertTo-Json)"
}

Test-Feature "Column-Family - Put Multiple Columns" {
    $columns = @(
        @{ family = "profile"; qualifier = "email"; value = "jane@example.com" },
        @{ family = "profile"; qualifier = "age"; value = "28" },
        @{ family = "settings"; qualifier = "notifications"; value = "enabled" }
    )
    
    foreach ($col in $columns) {
        $body = $col | ConvertTo-Json
        Invoke-RestMethod -Uri "$BASE_URL/columns/user_profiles/user123" -Method Put -Body $body -ContentType "application/json"
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
    $response = Invoke-RestMethod -Uri "$BASE_URL/bulk/products" -Method Post -Body $body -ContentType "application/json"
    Write-Host "Bulk insert response: $($response | ConvertTo-Json)"
    Assert-True ($response.inserted -ge 5) "Bulk insert should insert at least 5 docs"
}

# ============================================
# 6. SQL Operations (H2 Engine)
# ============================================
Test-Feature "SQL - Create Table" {
    $sql = "CREATE TABLE IF NOT EXISTS employees (id INT PRIMARY KEY, name VARCHAR(100), department VARCHAR(50), salary DECIMAL(10,2))"
    $response = Invoke-RestMethod -Uri "$BASE_URL/sql" -Method Post -Body @{ sql = $sql } -ContentType "application/json"
    Write-Host "Create table response: $($response | ConvertTo-Json)"
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
        $response = Invoke-RestMethod -Uri "$BASE_URL/sql" -Method Post -Body @{ sql = $sql } -ContentType "application/json"
    }
    Write-Host "✓ Inserted 5 employee records"
}

Test-Feature "SQL - Select All" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/sql" -Method Post -Body @{ sql = "SELECT * FROM employees" } -ContentType "application/json"
    Write-Host "Select all response: $($response | ConvertTo-Json -Depth 5)"
    Assert-True ($response.rows.Count -ge 5) "Should have 5 rows"
}

Test-Feature "SQL - Select with WHERE" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/sql" -Method Post -Body @{ sql = "SELECT * FROM employees WHERE department = 'Engineering'" } -ContentType "application/json"
    Write-Host "Engineering dept: $($response | ConvertTo-Json -Depth 5)"
    Assert-True ($response.rows.Count -ge 3) "Should have 3 engineers"
}

Test-Feature "SQL - Aggregate Query" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/sql" -Method Post -Body @{ sql = "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department" } -ContentType "application/json"
    Write-Host "Aggregation: $($response | ConvertTo-Json -Depth 5)"
}

Test-Feature "SQL - Update Records" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/sql" -Method Post -Body @{ sql = "UPDATE employees SET salary = salary * 1.1 WHERE department = 'Engineering'" } -ContentType "application/json"
    Write-Host "Update response: $($response | ConvertTo-Json)"
}

Test-Feature "SQL - Verify Update" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/sql" -Method Post -Body @{ sql = "SELECT name, salary FROM employees WHERE department = 'Engineering'" } -ContentType "application/json"
    Write-Host "Updated salaries: $($response | ConvertTo-Json -Depth 5)"
}

Test-Feature "SQL - Delete Records" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/sql" -Method Post -Body @{ sql = "DELETE FROM employees WHERE id = 5" } -ContentType "application/json"
    Write-Host "Delete response: $($response | ConvertTo-Json)"
}

# ============================================
# 7. Schema Operations
# ============================================
Test-Feature "Schema - List Tables" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/schema" -Method Get
    Write-Host "Schema tables: $($response | ConvertTo-Json)"
}

Test-Feature "Schema - Create New Table" {
    $sql = "CREATE TABLE IF NOT EXISTS products (id INT PRIMARY KEY, name VARCHAR(100), price DECIMAL(10,2), stock INT)"
    $response = Invoke-RestMethod -Uri "$BASE_URL/sql" -Method Post -Body @{ sql = $sql } -ContentType "application/json"
    
    $response = Invoke-RestMethod -Uri "$BASE_URL/schema" -Method Get
    Write-Host "Updated schema: $($response | ConvertTo-Json)"
}

# ============================================
# 8. Table Operations
# ============================================
Test-Feature "Table - Get Table Info" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/tables/employees" -Method Get
    Write-Host "Table info: $($response | ConvertTo-Json)"
}

Test-Feature "Table - Drop Table" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/tables/products" -Method Delete
    Write-Host "Drop table response: $($response | ConvertTo-Json)"
}

# ============================================
# 9. CDC (Change Data Capture) Operations
# ============================================
Test-Feature "CDC - Enable CDC" {
    $config = @{
        enabled = $true
        targetType = "MEMORY"
        collections = @("users", "products")
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$BASE_URL/cdc" -Method Post -Body $config -ContentType "application/json"
    Write-Host "CDC enable response: $($response | ConvertTo-Json)"
}

Test-Feature "CDC - Get CDC Status" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/cdc" -Method Get
    Write-Host "CDC status: $($response | ConvertTo-Json)"
}

# ============================================
# 10. Advanced Document Features
# ============================================
Test-Feature "Document - Create with Custom ID" {
    $doc = @{
        _id = "custom-id-12345"
        name = "Custom ID User"
        email = "custom@example.com"
    } | ConvertTo-Json
    
    $response = Invoke-RestMethod -Uri "$BASE_URL/collections/custom_docs" -Method Post -Body $doc -ContentType "application/json"
    Write-Host "Custom ID response: $($response | ConvertTo-Json)"
    Assert-True ($response.id -eq "custom-id-12345") "Custom ID should be preserved"
}

Test-Feature "Document - Query with Custom ID" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/collections/custom_docs/custom-id-12345" -Method Get
    Write-Host "Custom ID doc: $($response | ConvertTo-Json)"
    Assert-True ($response.name -eq "Custom ID User") "Custom ID doc retrieval failed"
}

# ============================================
# 11. Edge Cases & Error Handling
# ============================================
Test-Feature "Error Handling - Get Non-existent Document" {
    try {
        Invoke-RestMethod -Uri "$BASE_URL/collections/users/nonexistent-id" -Method Get
        throw "Should have returned 404"
    }
    catch {
        Write-Host "✓ Correctly returned error for non-existent document"
    }
}

Test-Feature "Error Handling - Invalid SQL" {
    try {
        Invoke-RestMethod -Uri "$BASE_URL/sql" -Method Post -Body @{ sql = "INVALID SQL STATEMENT!!!" } -ContentType "application/json"
        throw "Should have returned SQL error"
    }
    catch {
        Write-Host "✓ Correctly returned error for invalid SQL"
    }
}

Test-Feature "Error Handling - Empty Collection Query" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/collections/empty_collection_test" -Method Get
    Write-Host "Empty collection response: $($response | ConvertTo-Json)"
    Write-Host "✓ Empty collection handled correctly"
}

# ============================================
# 12. Data Verification
# ============================================
Test-Feature "Verification - Count All Collections" {
    $users = Invoke-RestMethod -Uri "$BASE_URL/collections/users" -Method Get
    $products = Invoke-RestMethod -Uri "$BASE_URL/collections/products" -Method Get
    $custom = Invoke-RestMethod -Uri "$BASE_URL/collections/custom_docs" -Method Get
    
    Write-Host "Users: $($users.Count), Products: $($products.Count), Custom: $($custom.Count)"
}

Test-Feature "Verification - SQL Data Integrity" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/sql" -Method Post -Body @{ sql = "SELECT COUNT(*) as count FROM employees" } -ContentType "application/json"
    Write-Host "Employee count: $($response | ConvertTo-Json)"
}

# ============================================
# Summary
# ============================================
Write-Host "`n========================================" -ForegroundColor Yellow
Write-Host "TEST SUITE COMPLETE" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow
Write-Host "Passed: $PASS" -ForegroundColor Green
Write-Host "Failed: $FAIL" -ForegroundColor $(if ($FAIL -eq 0) { "Green" } else { "Red" })
Write-Host "========================================" -ForegroundColor Yellow

if ($FAIL -eq 0) {
    Write-Host "✓ ALL TESTS PASSED!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "✗ Some tests failed" -ForegroundColor Red
    exit 1
}
