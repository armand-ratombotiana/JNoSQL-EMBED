import re

file_path = r'c:\Users\jratombo-adm\Desktop\JNoSQL-EMBED\src\main\java\org\junify\db\console\http\JunifyDBServer.java'

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Add sinter, sunion, sdiff to SetHandler
srandmember_pattern = r'(case "srandmember", "randmember" -> \{[^}]+\}\s+case "stats" ->)'
srandmember_replacement = '''case "srandmember", "randmember" -> {
                    if (!"GET".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "GET required"));
                        return;
                    }
                    var query = exchange.getRequestURI().getQuery();
                    int count = 1;
                    if (query != null) {
                        for (String param : query.split("&")) {
                            var kv = param.split("=");
                            if (kv.length == 2 && "count".equals(kv[0])) {
                                count = Integer.parseInt(kv[1]);
                            }
                        }
                    }
                    if (count == 1) {
                        String member = bucket.srandmember(key);
                        sendJson(exchange, 200, Map.of("key", key, "operation", "srandmember", "member", member));
                    } else {
                        var members = bucket.srandmember(key, count);
                        sendJson(exchange, 200, Map.of("key", key, "operation", "srandmember", "members", members));
                    }
                }
                case "sinter", "inter" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    var keys = parseStringArray(data.get("keys"));
                    var result = bucket.sinter(keys);
                    sendJson(exchange, 200, Map.of("operation", "sinter", "keys", java.util.Arrays.toString(keys), "intersection", result));
                }
                case "sunion", "union" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    var keys = parseStringArray(data.get("keys"));
                    var result = bucket.sunion(keys);
                    sendJson(exchange, 200, Map.of("operation", "sunion", "keys", java.util.Arrays.toString(keys), "union", result));
                }
                case "sdiff", "diff" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    var keys = parseStringArray(data.get("keys"));
                    var result = bucket.sdiff(keys);
                    sendJson(exchange, 200, Map.of("operation", "sdiff", "keys", java.util.Arrays.toString(keys), "difference", result));
                }
                case "stats" ->'''

content = re.sub(srandmember_pattern, srandmember_replacement, content, flags=re.DOTALL)

# Update the supported operations list in SetHandler default case
set_supported_pattern = r'("supported", "sadd, srem, smembers, sismember, scard, spop, srandmember, stats")'
set_supported_replacement = '"supported", "sadd, srem, smembers, sismember, scard, spop, srandmember, sinter, sunion, sdiff, stats"'
content = re.sub(set_supported_pattern, set_supported_replacement, content)

# Add hincrbyfloat to HashHandler
hash_hincrby_pattern = r'(case "hincrby", "incrby" -> \{[^}]+\}[^}]+\})(\s+case "stats" ->)'
hash_hincrby_replacement = '''case "hincrby", "incrby" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    String field = data.get("field").toString();
                    long delta = data.containsKey("delta") ? ((Number) data.get("delta")).longValue() : 1;
                    long newValue = bucket.hincrby(key, field, delta);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "hincrby", "field", field, "newValue", newValue));
                }
                case "hincrbyfloat", "incrbyfloat" -> {
                    if (!"POST".equals(exchange.getRequestMethod())) {
                        sendJson(exchange, 405, Map.of("error", "POST required"));
                        return;
                    }
                    var body = readBody(exchange);
                    var data = JsonSerde.fromJson(body, Map.class);
                    String field = data.get("field").toString();
                    double delta = data.containsKey("delta") ? ((Number) data.get("delta")).doubleValue() : 1.0;
                    String newValue = bucket.hincrbyfloat(key, field, delta);
                    sendJson(exchange, 200, Map.of("key", key, "operation", "hincrbyfloat", "field", field, "newValue", newValue));
                }
                case "stats" ->'''

content = re.sub(hash_hincrby_pattern, hash_hincrby_replacement, content, flags=re.DOTALL)

# Update the supported operations list in HashHandler default case
hash_supported_pattern = r'("supported", "hset, hget, hgetall, hdel, hlen, hexists, hkeys, hvals, hmget, hincrby, stats")'
hash_supported_replacement = '"supported", "hset, hget, hgetall, hdel, hlen, hexists, hkeys, hvals, hmget, hincrby, hincrbyfloat, stats"'
content = re.sub(hash_supported_pattern, hash_supported_replacement, content)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("Successfully updated JunifyDBServer.java with new REST endpoints!")
print("Added:")
print("  - Set operations: sinter, sunion, sdiff")
print("  - Hash operation: hincrbyfloat")
