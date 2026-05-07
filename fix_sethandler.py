import re

file_path = r'c:\Users\jratombo-adm\Desktop\JNoSQL-EMBED\src\main\java\org\junify\db\console\http\JunifyDBServer.java'

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Find the location after srandmember case and before stats case in SetHandler
# We need to insert sinter, sunion, sdiff there
srandmember_end = content.find('case "stats" -> {', content.find('case "srandmember", "randmember" ->'))

if srandmember_end != -1:
    # Find the start of the line
    line_start = content.rfind('\n', 0, srandmember_end) + 1
    
    set_operations_code = '''                case "sinter", "inter" -> {
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
'''
    
    content = content[:line_start] + set_operations_code + content[line_start:]

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("Successfully added sinter, sunion, sdiff endpoints to SetHandler!")
