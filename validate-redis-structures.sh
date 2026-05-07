#!/bin/bash
# Redis-Style Data Structures Validation Script for JunifyDB
# Run this after starting the server: java -jar junify-embed.jar --port 8080

BASE_URL="http://localhost:8080/api/kv"
LIST_URL="$BASE_URL/lists/demo"
SET_URL="$BASE_URL/sets/demo"
HASH_URL="$BASE_URL/hashes/demo"

echo "=============================================="
echo "=== REDIS-STYLE DATA STRUCTURES VALIDATION ==="
echo "=============================================="
echo ""

# --- LIST OPERATIONS ---
echo "=== LIST BUCKET OPERATIONS ==="
echo ""

echo "1. LPUSH - Add elements to left side:"
curl -s -X POST "$LIST_URL/mylist/lpush" \
  -H "Content-Type: application/json" \
  -d '{"values": ["c", "b", "a"]}'
echo -e "\n"

echo "2. RPUSH - Add elements to right side:"
curl -s -X POST "$LIST_URL/mylist/rpush" \
  -H "Content-Type: application/json" \
  -d '{"values": ["d", "e", "f"]}'
echo -e "\n"

echo "3. LLEN - Get list length:"
curl -s -X GET "$LIST_URL/mylist/len"
echo -e "\n"

echo "4. LRANGE - Get all elements (0 to -1):"
curl -s -X GET "$LIST_URL/mylist/range?start=0&end=-1"
echo -e "\n"

echo "5. LRANGE - Get partial range (1 to 3):"
curl -s -X GET "$LIST_URL/mylist/range?start=1&end=3"
echo -e "\n"

echo "6. LINDEX - Get element at index 2:"
curl -s -X GET "$LIST_URL/mylist/lindex?index=2"
echo -e "\n"

echo "7. LPOP - Remove from left:"
curl -s -X POST "$LIST_URL/mylist/lpop"
echo -e "\n"

echo "8. RPOP - Remove from right:"
curl -s -X POST "$LIST_URL/mylist/rpop"
echo -e "\n"

echo "9. List after pops:"
curl -s -X GET "$LIST_URL/mylist/range?start=0&end=-1"
echo -e "\n"

echo "10. LREM - Remove occurrences of 'c':"
curl -s -X POST "$LIST_URL/mylist/lrem" \
  -H "Content-Type: application/json" \
  -d '{"count": 0, "value": "c"}'
echo -e "\n"

echo "11. LTRIM - Trim to range [0, 2]:"
curl -s -X POST "$LIST_URL/mylist/ltrim" \
  -H "Content-Type: application/json" \
  -d '{"start": 0, "end": 2}'
echo -e "\n"

echo "12. Final list state:"
curl -s -X GET "$LIST_URL/mylist"
echo -e "\n"

echo "13. List stats:"
curl -s -X GET "$LIST_URL/mylist/stats"
echo -e "\n"

# --- SET OPERATIONS ---
echo "=== SET BUCKET OPERATIONS ==="
echo ""

echo "1. SADD - Add members:"
curl -s -X POST "$SET_URL/myset/sadd" \
  -H "Content-Type: application/json" \
  -d '{"members": ["apple", "banana", "cherry", "date"]}'
echo -e "\n"

echo "2. SADD - Try adding duplicates:"
curl -s -X POST "$SET_URL/myset/sadd" \
  -H "Content-Type: application/json" \
  -d '{"members": ["apple", "elderberry"]}'
echo -e "\n"

echo "3. SCARD - Get cardinality:"
curl -s -X GET "$SET_URL/myset/scard"
echo -e "\n"

echo "4. SMEMBERS - Get all members:"
curl -s -X GET "$SET_URL/myset/smembers"
echo -e "\n"

echo "5. SISMEMBER - Check if 'banana' exists:"
curl -s -X GET "$SET_URL/myset/sismember?member=banana"
echo -e "\n"

echo "6. SISMEMBER - Check if 'fig' exists:"
curl -s -X GET "$SET_URL/myset/sismember?member=fig"
echo -e "\n"

echo "7. SPOP - Pop one random member:"
curl -s -X POST "$SET_URL/myset/spop" \
  -H "Content-Type: application/json" \
  -d '{}'
echo -e "\n"

echo "8. SPOP - Pop multiple members:"
curl -s -X POST "$SET_URL/myset/spop" \
  -H "Content-Type: application/json" \
  -d '{"count": 2}'
echo -e "\n"

echo "9. Set after pops:"
curl -s -X GET "$SET_URL/myset/smembers"
echo -e "\n"

echo "10. SREM - Remove specific members:"
curl -s -X POST "$SET_URL/myset/srem" \
  -H "Content-Type: application/json" \
  -d '{"members": ["cherry"]}'
echo -e "\n"

echo "11. Final set state:"
curl -s -X GET "$SET_URL/myset"
echo -e "\n"

echo "12. Set stats:"
curl -s -X GET "$SET_URL/myset/stats"
echo -e "\n"

# --- HASH OPERATIONS ---
echo "=== HASH BUCKET OPERATIONS ==="
echo ""

echo "1. HSET - Set single field:"
curl -s -X POST "$HASH_URL/user:hset" \
  -H "Content-Type: application/json" \
  -d '{"field": "name", "value": "John Doe"}'
echo -e "\n"

echo "2. HSET - Set multiple fields:"
curl -s -X POST "$HASH_URL/user/hset" \
  -H "Content-Type: application/json" \
  -d '{"fields": {"age": "30", "email": "john@example.com", "city": "New York"}}'
echo -e "\n"

echo "3. HGET - Get single field:"
curl -s -X GET "$HASH_URL/user/hget?field=name"
echo -e "\n"

echo "4. HGETALL - Get all fields:"
curl -s -X GET "$HASH_URL/user/getall"
echo -e "\n"

echo "5. HLEN - Get field count:"
curl -s -X GET "$HASH_URL/user/len"
echo -e "\n"

echo "6. HEXISTS - Check if 'email' exists:"
curl -s -X GET "$HASH_URL/user/exists?field=email"
echo -e "\n"

echo "7. HEXISTS - Check if 'phone' exists:"
curl -s -X GET "$HASH_URL/user/exists?field=phone"
echo -e "\n"

echo "8. HKEYS - Get all field names:"
curl -s -X GET "$HASH_URL/user/keys"
echo -e "\n"

echo "9. HVALS - Get all values:"
curl -s -X GET "$HASH_URL/user/vals"
echo -e "\n"

echo "10. HMGET - Get multiple fields:"
curl -s -X POST "$HASH_URL/user/mget" \
  -H "Content-Type: application/json" \
  -d '{"fields": ["name", "email", "city"]}'
echo -e "\n"

echo "11. HINCRBY - Increment field:"
curl -s -X POST "$HASH_URL/user/incrby" \
  -H "Content-Type: application/json" \
  -d '{"field": "loginCount", "delta": 5}'
echo -e "\n"

echo "12. HINCRBY - Increment again:"
curl -s -X POST "$HASH_URL/user/incrby" \
  -H "Content-Type: application/json" \
  -d '{"field": "loginCount", "delta": 3}'
echo -e "\n"

echo "13. HDEL - Delete fields:"
curl -s -X POST "$HASH_URL/user/del" \
  -H "Content-Type: application/json" \
  -d '{"fields": ["city"]}'
echo -e "\n"

echo "14. Final hash state:"
curl -s -X GET "$HASH_URL/user"
echo -e "\n"

echo "15. Hash stats:"
curl -s -X GET "$HASH_URL/user/stats"
echo -e "\n"

echo "=============================================="
echo "=== VALIDATION COMPLETE ==="
echo "=============================================="
