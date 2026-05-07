#!/bin/bash
# ColumnFamily Advanced Features Validation Script
# Run this after starting the server: java -jar junify-db-core.jar --port 8080

BASE_URL="http://localhost:8080"
FAMILY="demo_family"

echo "=============================================="
echo "COLUMN-FAMILY ADVANCED FEATURES VALIDATION"
echo "=============================================="
echo ""

# 1. Create test data with TTL
echo "1. Creating test data with TTL..."
curl -s -X POST "$BASE_URL/api/columns/$FAMILY/user:1" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Alice",
    "email": "alice@example.com",
    "age": 30,
    "session_token": {"value": "abc123", "ttlSeconds": 3600}
  }' | jq .
echo ""

# 2. Get full row
echo "2. Get full row..."
curl -s "$BASE_URL/api/columns/$FAMILY/user:1" | jq .
echo ""

# 3. Get row with pagination
echo "3. Get row with pagination (limit=2, offset=0)..."
curl -s "$BASE_URL/api/columns/$FAMILY/user:1/get-range?limit=2&offset=0" | jq .
echo ""

# 4. Filter columns by name
echo "4. Filter columns by name..."
curl -s "$BASE_URL/api/columns/$FAMILY/user:1/filter?columns=name,email" | jq .
echo ""

# 5. Filter by prefix
echo "5. Create time-series data and filter by prefix..."
for i in {0..9}; do
  curl -s -X POST "$BASE_URL/api/columns/$FAMILY/metrics:cpu" \
    -H "Content-Type: application/json" \
    -d "{\"ts:00$i\": $(echo $RANDOM)}" > /dev/null
done
curl -s "$BASE_URL/api/columns/$FAMILY/metrics:cpu/filter-prefix?prefix=ts:00" | jq .
echo ""

# 6. Get TTL for a column
echo "6. Get TTL for session_token column..."
curl -s "$BASE_URL/api/columns/$FAMILY/user:1/ttl/session_token" | jq .
echo ""

# 7. Get family statistics
echo "7. Get column family statistics..."
curl -s "$BASE_URL/api/columns/$FAMILY/stats" | jq .
echo ""

# 8. Get row statistics
echo "8. Get row statistics..."
curl -s "$BASE_URL/api/columns/$FAMILY/user:1/stats" | jq .
echo ""

# 9. Put single column with TTL
echo "9. Put single column with TTL..."
curl -s -X PUT "$BASE_URL/api/columns/$FAMILY/user:1/column/temp_data" \
  -H "Content-Type: application/json" \
  -d '{"value": "temporary", "ttlSeconds": 60}' | jq .
echo ""

# 10. Verify TTL was set
echo "10. Verify TTL was set..."
curl -s "$BASE_URL/api/columns/$FAMILY/user:1/ttl/temp_data" | jq .
echo ""

# 11. Get row slice (multiple rows)
echo "11. Get row slice..."
curl -s "$BASE_URL/api/columns/$FAMILY/metrics:cpu/get-range?limit=5&offset=0" | jq .
echo ""

# 12. Filter by regex pattern
echo "12. Filter by regex pattern..."
curl -s "$BASE_URL/api/columns/$FAMILY/metrics:cpu/filter-pattern?pattern=ts:.*" | jq .
echo ""

echo "=============================================="
echo "VALIDATION COMPLETE"
echo "=============================================="
