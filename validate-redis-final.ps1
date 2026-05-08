# JunifyDB Redis-Style Data Structures Validation Script
# Tests List, Set, and Hash operations with correct API endpoints

$BASE_URL = "http://localhost:8080/api"
$API_KEY = "hYXuECpj4dM28vf3En47ar2KaA1FPMLVNrmAYYSAoFM"
$HEADERS = @{ "X-API-Key" = $API_KEY; "Content-Type" = "application/json" }
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

# ============================================
# LIST BUCKET OPERATIONS
# ============================================
Write-Host "`n========================================" -ForegroundColor Yellow
Write-Host "=== LIST BUCKET OPERATIONS ===" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow

Test-Feature "List - LPUSH (add elements to left)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/mylist/lpush" -Headers $HEADERS -Method Post -Body '{"values": ["c", "b", "a"]}'
    Write-Host "LPUSH response: length=$($response.length)"
    if ($response.length -ne 3) { throw "Expected length 3, got $($response.length)" }
}

Test-Feature "List - RPUSH (add elements to right)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/mylist/rpush" -Headers $HEADERS -Method Post -Body '{"values": ["d", "e", "f"]}'
    Write-Host "RPUSH response: length=$($response.length)"
    if ($response.length -ne 6) { throw "Expected length 6, got $($response.length)" }
}

Test-Feature "List - LLEN (get length)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/mylist/len" -Headers $HEADERS -Method Get
    Write-Host "LLEN response: length=$($response.length)"
    if ($response.length -ne 6) { throw "Expected length 6, got $($response.length)" }
}

Test-Feature "List - LRANGE (get all elements 0 to -1)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/mylist/range?start=0&end=-1" -Headers $HEADERS -Method Get
    Write-Host "LRANGE response: $($response.values.Count) elements"
    if ($response.values.Count -lt 1) { throw "Expected elements" }
}

Test-Feature "List - LRANGE (partial range 1 to 3)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/mylist/range?start=1&end=3" -Headers $HEADERS -Method Get
    Write-Host "LRANGE [1-3]: $($response.values.Count) elements"
}

Test-Feature "List - LINDEX (get element at index 2)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/mylist/lindex?index=2" -Headers $HEADERS -Method Get
    Write-Host "LINDEX [2]: value=$($response.value)"
}

Test-Feature "List - LPOP (remove from left)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/mylist/lpop" -Headers $HEADERS -Method Post -Body '{}'
    Write-Host "LPOP response: value=$($response.value)"
}

Test-Feature "List - RPOP (remove from right)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/mylist/rpop" -Headers $HEADERS -Method Post -Body '{}'
    Write-Host "RPOP response: value=$($response.value)"
}

Test-Feature "List - List after pops" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/mylist/range?start=0&end=-1" -Headers $HEADERS -Method Get
    Write-Host "Remaining elements: $($response.values.Count)"
}

Test-Feature "List - LREM (remove occurrences)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/mylist/lrem" -Headers $HEADERS -Method Post -Body '{"count": 0, "value": "c"}'
    Write-Host "LREM response: removed=$($response.removed)"
}

Test-Feature "List - LTRIM (trim to range)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/mylist/ltrim" -Headers $HEADERS -Method Post -Body '{"start": 0, "end": 2}'
    Write-Host "LTRIM response: start=$($response.start), end=$($response.end)"
}

Test-Feature "List - Final state" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/mylist" -Headers $HEADERS -Method Get
    Write-Host "Final list: $($response.values.Count) elements"
}

Test-Feature "List - Stats" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/lists/demo/stats" -Headers $HEADERS -Method Get
    Write-Host "List stats retrieved"
}

# ============================================
# SET BUCKET OPERATIONS
# ============================================
Write-Host "`n========================================" -ForegroundColor Yellow
Write-Host "=== SET BUCKET OPERATIONS ===" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow

Test-Feature "Set - SADD (add members)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/sets/demo/myset/sadd" -Headers $HEADERS -Method Post -Body '{"members": ["apple", "banana", "cherry", "date"]}'
    Write-Host "SADD response: added=$($response.added)"
}

Test-Feature "Set - SADD (duplicates)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/sets/demo/myset/sadd" -Headers $HEADERS -Method Post -Body '{"members": ["apple", "elderberry"]}'
    Write-Host "SADD duplicates: added=$($response.added) (apple should not be added)"
}

Test-Feature "Set - SCARD (get cardinality)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/sets/demo/myset/scard" -Headers $HEADERS -Method Get
    Write-Host "SCARD response: cardinality=$($response.cardinality)"
}

Test-Feature "Set - SMEMBERS (get all members)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/sets/demo/myset/smembers" -Headers $HEADERS -Method Get
    Write-Host "SMEMBERS response: $($response.members.Count) members"
}

Test-Feature "Set - SISMEMBER (check if banana exists)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/sets/demo/myset/sismember?member=banana" -Headers $HEADERS -Method Get
    Write-Host "SISMEMBER (banana): exists=$($response.exists)"
}

Test-Feature "Set - SISMEMBER (check if fig exists)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/sets/demo/myset/sismember?member=fig" -Headers $HEADERS -Method Get
    Write-Host "SISMEMBER (fig): exists=$($response.exists)"
}

Test-Feature "Set - SPOP (pop one random member)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/sets/demo/myset/spop" -Headers $HEADERS -Method Post -Body '{}'
    Write-Host "SPOP response: popped=$($response.popped)"
}

Test-Feature "Set - SPOP (pop multiple members)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/sets/demo/myset/spop" -Headers $HEADERS -Method Post -Body '{"count": 2}'
    Write-Host "SPOP (count=2): popped=$($response.popped.Count) members"
}

Test-Feature "Set - Set after pops" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/sets/demo/myset/smembers" -Headers $HEADERS -Method Get
    Write-Host "Remaining members: $($response.members.Count)"
}

Test-Feature "Set - SREM (remove specific members)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/sets/demo/myset/srem" -Headers $HEADERS -Method Post -Body '{"members": ["cherry"]}'
    Write-Host "SREM response: removed=$($response.removed)"
}

Test-Feature "Set - Final state" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/sets/demo/myset" -Headers $HEADERS -Method Get
    Write-Host "Final set: $($response.members.Count) members"
}

Test-Feature "Set - Stats" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/sets/demo/stats" -Headers $HEADERS -Method Get
    Write-Host "Set stats retrieved"
}

# ============================================
# HASH BUCKET OPERATIONS
# ============================================
Write-Host "`n========================================" -ForegroundColor Yellow
Write-Host "=== HASH BUCKET OPERATIONS ===" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow

Test-Feature "Hash - HSET (single field)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/hset" -Headers $HEADERS -Method Post -Body '{"field": "name", "value": "John Doe"}'
    Write-Host "HSET response: fieldsSet=$($response.fieldsSet)"
}

Test-Feature "Hash - HSET (multiple fields)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/hset" -Headers $HEADERS -Method Post -Body '{"fields": {"age": "30", "email": "john@example.com", "city": "New York"}}'
    Write-Host "HSET multiple response: fieldsSet=$($response.fieldsSet)"
}

Test-Feature "Hash - HGET (single field)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/hget?field=name" -Headers $HEADERS -Method Get
    Write-Host "HGET response: value=$($response.value)"
    if ($response.value -ne "John Doe") { throw "Expected 'John Doe', got $($response.value)" }
}

Test-Feature "Hash - HGETALL (get all fields)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/getall" -Headers $HEADERS -Method Get
    Write-Host "HGETALL response: $($response.fields.Count) fields"
}

Test-Feature "Hash - HLEN (get field count)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/len" -Headers $HEADERS -Method Get
    Write-Host "HLEN response: length=$($response.length)"
}

Test-Feature "Hash - HEXISTS (check if email exists)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/exists?field=email" -Headers $HEADERS -Method Get
    Write-Host "HEXISTS (email): exists=$($response.exists)"
}

Test-Feature "Hash - HEXISTS (check if phone exists)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/exists?field=phone" -Headers $HEADERS -Method Get
    Write-Host "HEXISTS (phone): exists=$($response.exists)"
}

Test-Feature "Hash - HKEYS (get all field names)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/keys" -Headers $HEADERS -Method Get
    Write-Host "HKEYS response: $($response.keys.Count) keys"
}

Test-Feature "Hash - HVALS (get all values)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/vals" -Headers $HEADERS -Method Get
    Write-Host "HVALS response: $($response.values.Count) values"
}

Test-Feature "Hash - HMGET (get multiple fields)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/mget" -Headers $HEADERS -Method Post -Body '{"fields": ["name", "email", "city"]}'
    Write-Host "HMGET response: $($response.values.Count) values"
}

Test-Feature "Hash - HINCRBY (increment field)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/incrby" -Headers $HEADERS -Method Post -Body '{"field": "loginCount", "delta": 5}'
    Write-Host "HINCRBY response: newValue=$($response.newValue)"
}

Test-Feature "Hash - HINCRBY (increment again)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/incrby" -Headers $HEADERS -Method Post -Body '{"field": "loginCount", "delta": 3}'
    Write-Host "HINCRBY response: newValue=$($response.newValue)"
}

Test-Feature "Hash - HDEL (delete fields)" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user/del" -Headers $HEADERS -Method Post -Body '{"fields": ["city"]}'
    Write-Host "HDEL response: deleted=$($response.deleted)"
}

Test-Feature "Hash - Final state" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/user" -Headers $HEADERS -Method Get
    Write-Host "Final hash: $($response.fields.Count) fields"
}

Test-Feature "Hash - Stats" {
    $response = Invoke-RestMethod -Uri "$BASE_URL/kv/hashes/demo/stats" -Headers $HEADERS -Method Get
    Write-Host "Hash stats retrieved"
}

# ============================================
# SUMMARY
# ============================================
Write-Host "`n========================================" -ForegroundColor Yellow
Write-Host "=== VALIDATION COMPLETE ===" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow
Write-Host "Passed: $PASS" -ForegroundColor Green
Write-Host "Failed: $FAIL" -ForegroundColor $(if ($FAIL -eq 0) { "Green" } else { "Red" })
Write-Host "Total:  $($PASS + $FAIL)" -ForegroundColor Yellow
Write-Host "========================================" -ForegroundColor Yellow

if ($FAIL -eq 0) {
    Write-Host "✓ ALL REDIS-STYLE TESTS PASSED!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "✗ Some tests failed" -ForegroundColor Red
    exit 1
}
