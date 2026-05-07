# Redis-Style Data Structures Validation Script for JunifyDB (PowerShell)
# Run this after starting the server: java -jar junify-embed.jar --port 8080

$BASE_URL = "http://localhost:8080/api/kv"
$LIST_URL = "$BASE_URL/lists/demo"
$SET_URL = "$BASE_URL/sets/demo"
$HASH_URL = "$BASE_URL/hashes/demo"

Write-Host "=============================================="
Write-Host "=== REDIS-STYLE DATA STRUCTURES VALIDATION ==="
Write-Host "=============================================="
Write-Host ""

# --- LIST OPERATIONS ---
Write-Host "=== LIST BUCKET OPERATIONS ==="
Write-Host ""

Write-Host "1. LPUSH - Add elements to left side:"
Invoke-RestMethod -Method Post -Uri "$LIST_URL/mylist/lpush" -ContentType "application/json" -Body '{"values": ["c", "b", "a"]}' | ConvertTo-Json
Write-Host ""

Write-Host "2. RPUSH - Add elements to right side:"
Invoke-RestMethod -Method Post -Uri "$LIST_URL/mylist/rpush" -ContentType "application/json" -Body '{"values": ["d", "e", "f"]}' | ConvertTo-Json
Write-Host ""

Write-Host "3. LLEN - Get list length:"
Invoke-RestMethod -Method Get -Uri "$LIST_URL/mylist/len" | ConvertTo-Json
Write-Host ""

Write-Host "4. LRANGE - Get all elements (0 to -1):"
Invoke-RestMethod -Method Get -Uri "$LIST_URL/mylist/range?start=0&end=-1" | ConvertTo-Json
Write-Host ""

Write-Host "5. LRANGE - Get partial range (1 to 3):"
Invoke-RestMethod -Method Get -Uri "$LIST_URL/mylist/range?start=1&end=3" | ConvertTo-Json
Write-Host ""

Write-Host "6. LINDEX - Get element at index 2:"
Invoke-RestMethod -Method Get -Uri "$LIST_URL/mylist/lindex?index=2" | ConvertTo-Json
Write-Host ""

Write-Host "7. LPOP - Remove from left:"
Invoke-RestMethod -Method Post -Uri "$LIST_URL/mylist/lpop" -ContentType "application/json" -Body '{}' | ConvertTo-Json
Write-Host ""

Write-Host "8. RPOP - Remove from right:"
Invoke-RestMethod -Method Post -Uri "$LIST_URL/mylist/rpop" -ContentType "application/json" -Body '{}' | ConvertTo-Json
Write-Host ""

Write-Host "9. List after pops:"
Invoke-RestMethod -Method Get -Uri "$LIST_URL/mylist/range?start=0&end=-1" | ConvertTo-Json
Write-Host ""

Write-Host "10. LREM - Remove occurrences of 'c':"
Invoke-RestMethod -Method Post -Uri "$LIST_URL/mylist/lrem" -ContentType "application/json" -Body '{"count": 0, "value": "c"}' | ConvertTo-Json
Write-Host ""

Write-Host "11. LTRIM - Trim to range [0, 2]:"
Invoke-RestMethod -Method Post -Uri "$LIST_URL/mylist/ltrim" -ContentType "application/json" -Body '{"start": 0, "end": 2}' | ConvertTo-Json
Write-Host ""

Write-Host "12. Final list state:"
Invoke-RestMethod -Method Get -Uri "$LIST_URL/mylist" | ConvertTo-Json
Write-Host ""

Write-Host "13. List stats:"
Invoke-RestMethod -Method Get -Uri "$LIST_URL/mylist/stats" | ConvertTo-Json
Write-Host ""

# --- SET OPERATIONS ---
Write-Host "=== SET BUCKET OPERATIONS ==="
Write-Host ""

Write-Host "1. SADD - Add members:"
Invoke-RestMethod -Method Post -Uri "$SET_URL/myset/sadd" -ContentType "application/json" -Body '{"members": ["apple", "banana", "cherry", "date"]}' | ConvertTo-Json
Write-Host ""

Write-Host "2. SADD - Try adding duplicates:"
Invoke-RestMethod -Method Post -Uri "$SET_URL/myset/sadd" -ContentType "application/json" -Body '{"members": ["apple", "elderberry"]}' | ConvertTo-Json
Write-Host ""

Write-Host "3. SCARD - Get cardinality:"
Invoke-RestMethod -Method Get -Uri "$SET_URL/myset/scard" | ConvertTo-Json
Write-Host ""

Write-Host "4. SMEMBERS - Get all members:"
Invoke-RestMethod -Method Get -Uri "$SET_URL/myset/smembers" | ConvertTo-Json
Write-Host ""

Write-Host "5. SISMEMBER - Check if 'banana' exists:"
Invoke-RestMethod -Method Get -Uri "$SET_URL/myset/sismember?member=banana" | ConvertTo-Json
Write-Host ""

Write-Host "6. SISMEMBER - Check if 'fig' exists:"
Invoke-RestMethod -Method Get -Uri "$SET_URL/myset/sismember?member=fig" | ConvertTo-Json
Write-Host ""

Write-Host "7. SPOP - Pop one random member:"
Invoke-RestMethod -Method Post -Uri "$SET_URL/myset/spop" -ContentType "application/json" -Body '{}' | ConvertTo-Json
Write-Host ""

Write-Host "8. SPOP - Pop multiple members:"
Invoke-RestMethod -Method Post -Uri "$SET_URL/myset/spop" -ContentType "application/json" -Body '{"count": 2}' | ConvertTo-Json
Write-Host ""

Write-Host "9. Set after pops:"
Invoke-RestMethod -Method Get -Uri "$SET_URL/myset/smembers" | ConvertTo-Json
Write-Host ""

Write-Host "10. SREM - Remove specific members:"
Invoke-RestMethod -Method Post -Uri "$SET_URL/myset/srem" -ContentType "application/json" -Body '{"members": ["cherry"]}' | ConvertTo-Json
Write-Host ""

Write-Host "11. Final set state:"
Invoke-RestMethod -Method Get -Uri "$SET_URL/myset" | ConvertTo-Json
Write-Host ""

Write-Host "12. Set stats:"
Invoke-RestMethod -Method Get -Uri "$SET_URL/myset/stats" | ConvertTo-Json
Write-Host ""

# --- HASH OPERATIONS ---
Write-Host "=== HASH BUCKET OPERATIONS ==="
Write-Host ""

Write-Host "1. HSET - Set single field:"
Invoke-RestMethod -Method Post -Uri "$HASH_URL/user/hset" -ContentType "application/json" -Body '{"field": "name", "value": "John Doe"}' | ConvertTo-Json
Write-Host ""

Write-Host "2. HSET - Set multiple fields:"
Invoke-RestMethod -Method Post -Uri "$HASH_URL/user/hset" -ContentType "application/json" -Body '{"fields": {"age": "30", "email": "john@example.com", "city": "New York"}}' | ConvertTo-Json
Write-Host ""

Write-Host "3. HGET - Get single field:"
Invoke-RestMethod -Method Get -Uri "$HASH_URL/user/hget?field=name" | ConvertTo-Json
Write-Host ""

Write-Host "4. HGETALL - Get all fields:"
Invoke-RestMethod -Method Get -Uri "$HASH_URL/user/getall" | ConvertTo-Json
Write-Host ""

Write-Host "5. HLEN - Get field count:"
Invoke-RestMethod -Method Get -Uri "$HASH_URL/user/len" | ConvertTo-Json
Write-Host ""

Write-Host "6. HEXISTS - Check if 'email' exists:"
Invoke-RestMethod -Method Get -Uri "$HASH_URL/user/exists?field=email" | ConvertTo-Json
Write-Host ""

Write-Host "7. HEXISTS - Check if 'phone' exists:"
Invoke-RestMethod -Method Get -Uri "$HASH_URL/user/exists?field=phone" | ConvertTo-Json
Write-Host ""

Write-Host "8. HKEYS - Get all field names:"
Invoke-RestMethod -Method Get -Uri "$HASH_URL/user/keys" | ConvertTo-Json
Write-Host ""

Write-Host "9. HVALS - Get all values:"
Invoke-RestMethod -Method Get -Uri "$HASH_URL/user/vals" | ConvertTo-Json
Write-Host ""

Write-Host "10. HMGET - Get multiple fields:"
Invoke-RestMethod -Method Post -Uri "$HASH_URL/user/mget" -ContentType "application/json" -Body '{"fields": ["name", "email", "city"]}' | ConvertTo-Json
Write-Host ""

Write-Host "11. HINCRBY - Increment field:"
Invoke-RestMethod -Method Post -Uri "$HASH_URL/user/incrby" -ContentType "application/json" -Body '{"field": "loginCount", "delta": 5}' | ConvertTo-Json
Write-Host ""

Write-Host "12. HINCRBY - Increment again:"
Invoke-RestMethod -Method Post -Uri "$HASH_URL/user/incrby" -ContentType "application/json" -Body '{"field": "loginCount", "delta": 3}' | ConvertTo-Json
Write-Host ""

Write-Host "13. HDEL - Delete fields:"
Invoke-RestMethod -Method Post -Uri "$HASH_URL/user/del" -ContentType "application/json" -Body '{"fields": ["city"]}' | ConvertTo-Json
Write-Host ""

Write-Host "14. Final hash state:"
Invoke-RestMethod -Method Get -Uri "$HASH_URL/user" | ConvertTo-Json
Write-Host ""

Write-Host "15. Hash stats:"
Invoke-RestMethod -Method Get -Uri "$HASH_URL/user/stats" | ConvertTo-Json
Write-Host ""

Write-Host "=============================================="
Write-Host "=== VALIDATION COMPLETE ==="
Write-Host "=============================================="
