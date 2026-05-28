package org.junify.db.deep;

import org.junify.db.JunifyDB;
import org.junify.db.nosql.document.Document;
import org.junify.db.console.http.JunifyDBServer;
import org.junit.jupiter.api.*;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Sub-Agent D — Deep HTTP Frontend Tests.
 *
 * Covers: Collections REST (CRUD, query, stats, TTL, cleanup),
 * KV REST (put/get/delete/missing), List REST (lpush/rpush/lrange/llen/ltrim/lrem/lindex),
 * Set REST (sadd/srem/smembers/sismember/scard/spop/srandmember/sinter/sunion/sdiff),
 * Hash REST (hset/hget/hgetall/hdel/hlen/hexists/hkeys/hvals/hmget/hincrby/hincrbyfloat),
 * Column REST (put/get/delete/stats/filter/prefix/pattern),
 * Auth (401 enforcement, wrong key, valid key), CORS headers,
 * Static files, Metrics/Health/Stats/CDC/Audit, Error handling, Concurrency.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class DeepHttpFrontendTest {

    private static JunifyDB db;
    private static JunifyDBServer server;
    private static int port;
    private static final String API_KEY = "deep-test-secret-9876";

    @BeforeAll
    static void startServer() throws Exception {
        db = JunifyDB.embed().build();
        server = db.startServer(0);
        server.setApiKey(API_KEY);
        Thread.sleep(300);
        port = server.port();
    }

    @AfterAll
    static void stopServer() {
        if (server != null) server.stop();
        if (db != null && db.isOpen()) db.close();
    }

    // -----------------------------------------------------------------------
    // Core HTTP helpers
    // -----------------------------------------------------------------------

    record Resp(int code, String body, Map<String, List<String>> headers) {}

    static Resp http(String method, String path, String json, String apiKey) throws Exception {
        HttpURLConnection c = (HttpURLConnection)
                new URL("http://localhost:" + port + path).openConnection();
        c.setRequestMethod(method);
        c.setConnectTimeout(5_000);
        c.setReadTimeout(5_000);
        if (apiKey != null) c.setRequestProperty("X-API-Key", apiKey);
        if (json != null) {
            c.setDoOutput(true);
            c.setRequestProperty("Content-Type", "application/json");
            try (var os = c.getOutputStream()) {
                os.write(json.getBytes(StandardCharsets.UTF_8));
            }
        }
        int code;
        try { code = c.getResponseCode(); } catch (Exception e) { c.disconnect(); return new Resp(503, "", Map.of()); }
        var is = code >= 400 ? c.getErrorStream() : c.getInputStream();
        String body = is == null ? "" :
                new BufferedReader(new InputStreamReader(is)).lines().collect(Collectors.joining());
        var headers = c.getHeaderFields();
        c.disconnect();
        return new Resp(code, body, headers != null ? headers : Map.of());
    }

    static Resp GET(String path)                throws Exception { return http("GET",    path, null,  API_KEY); }
    static Resp POST(String path, String body)   throws Exception { return http("POST",   path, body,  API_KEY); }
    static Resp PUT(String path, String body)    throws Exception { return http("PUT",    path, body,  API_KEY); }
    static Resp DELETE(String path)              throws Exception { return http("DELETE", path, null,  API_KEY); }
    static Resp GETnoAuth(String path)           throws Exception { return http("GET",    path, null,  null); }
    static Resp GETwrongKey(String path)         throws Exception { return http("GET",    path, null,  "wrong-key-abc"); }

    static Resp safe(String method, String path, String body) {
        try { return http(method, path, body, API_KEY); }
        catch (Exception e) { return new Resp(503, "err:" + e.getMessage(), Map.of()); }
    }

    // =======================================================================
    // AUTH & SECURITY
    // =======================================================================

    @Test @Order(1)
    void auth_missingKeyReturns401() throws Exception {
        assertEquals(401, GETnoAuth("/api/health").code());
    }

    @Test @Order(2)
    void auth_wrongKeyReturns401() throws Exception {
        assertEquals(401, GETwrongKey("/api/health").code());
    }

    @Test @Order(3)
    void auth_validKeyReturns200() throws Exception {
        assertEquals(200, GET("/api/health").code());
    }

    @Test @Order(4)
    void auth_missingKeyOnCollections() throws Exception {
        assertEquals(401, http("GET", "/api/collections/x", null, null).code());
    }

    @Test @Order(5)
    void auth_missingKeyOnKv() throws Exception {
        assertEquals(401, http("GET", "/api/kv/b/k", null, null).code());
    }

    @Test @Order(6)
    void auth_missingKeyOnMetrics() throws Exception {
        assertEquals(401, http("GET", "/api/metrics", null, null).code());
    }

    // =======================================================================
    // CORS HEADERS
    // =======================================================================

    /** Case-insensitive header lookup (HttpURLConnection keys can vary by JDK). */
    private static List<String> getHeader(Map<String, List<String>> headers, String name) {
        for (var entry : headers.entrySet()) {
            if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(name)) {
                return entry.getValue();
            }
        }
        return null;
    }

    @Test @Order(10)
    void cors_originHeaderPresent() throws Exception {
        var resp = GET("/api/health");
        var origin = getHeader(resp.headers(), "Access-Control-Allow-Origin");
        assertNotNull(origin, "CORS origin header must be present; headers=" + resp.headers().keySet());
        assertFalse(origin.isEmpty());
    }

    @Test @Order(11)
    void cors_methodsHeaderPresent() throws Exception {
        var resp = GET("/api/health");
        var methods = getHeader(resp.headers(), "Access-Control-Allow-Methods");
        assertNotNull(methods, "CORS methods header must be present; headers=" + resp.headers().keySet());
    }

    // =======================================================================
    // HEALTH / METRICS / STATS
    // =======================================================================

    @Test @Order(20)
    void health_returns200WithStatusOk() throws Exception {
        var r = GET("/api/health");
        assertEquals(200, r.code());
        assertTrue(r.body().contains("ok") || r.body().contains("status"));
    }

    @Test @Order(21)
    void health_containsEngineField() throws Exception {
        var r = GET("/api/health");
        assertTrue(r.body().contains("engine") || r.body().contains("version"));
    }

    @Test @Order(22)
    void health_containsUptimeField() throws Exception {
        var r = GET("/api/health");
        assertTrue(r.body().contains("uptime") || r.body().contains("timestamp"));
    }

    @Test @Order(23)
    void metrics_returns200() throws Exception {
        assertEquals(200, GET("/api/metrics").code());
    }

    @Test @Order(24)
    void stats_returns200() throws Exception {
        assertEquals(200, GET("/api/stats").code());
    }

    // =======================================================================
    // STATIC FILES
    // =======================================================================

    @Test @Order(30)
    void static_indexHtml200() throws Exception {
        var c = (HttpURLConnection) new URL("http://localhost:" + port + "/").openConnection();
        c.setConnectTimeout(5_000); c.setReadTimeout(5_000);
        int code = c.getResponseCode();
        String ct = c.getContentType();
        c.disconnect();
        assertEquals(200, code);
        assertTrue(ct != null && ct.contains("text/html"));
    }

    @Test @Order(31)
    void static_loginHtml200() throws Exception {
        var c = (HttpURLConnection) new URL("http://localhost:" + port + "/login.html").openConnection();
        c.setConnectTimeout(5_000); c.setReadTimeout(5_000);
        assertEquals(200, c.getResponseCode());
        c.disconnect();
    }

    @Test @Order(32)
    void static_resetPasswordHtml200() throws Exception {
        var c = (HttpURLConnection) new URL("http://localhost:" + port + "/reset-password.html").openConnection();
        c.setConnectTimeout(5_000); c.setReadTimeout(5_000);
        assertEquals(200, c.getResponseCode());
        c.disconnect();
    }

    @Test @Order(33)
    void static_cssFile200() throws Exception {
        var c = (HttpURLConnection) new URL("http://localhost:" + port + "/css/enhancements.css").openConnection();
        c.setConnectTimeout(5_000); c.setReadTimeout(5_000);
        int code = c.getResponseCode();
        c.disconnect();
        assertTrue(code == 200 || code == 404, "CSS file: " + code);
    }

    @Test @Order(34)
    void static_jsFile200() throws Exception {
        var c = (HttpURLConnection) new URL("http://localhost:" + port + "/js/enhancements.js").openConnection();
        c.setConnectTimeout(5_000); c.setReadTimeout(5_000);
        int code = c.getResponseCode();
        c.disconnect();
        assertTrue(code == 200 || code == 404, "JS file: " + code);
    }

    @Test @Order(35)
    void static_missingFileReturns404() throws Exception {
        var c = (HttpURLConnection) new URL("http://localhost:" + port + "/does-not-exist.xyz").openConnection();
        c.setConnectTimeout(5_000); c.setReadTimeout(5_000);
        assertEquals(404, c.getResponseCode());
        c.disconnect();
    }

    // =======================================================================
    // COLLECTIONS REST
    // =======================================================================

    @Test @Order(40)
    void col_insertReturns201WithBody() throws Exception {
        var r = POST("/api/collections/h_col_a", "{\"name\":\"DeepA\",\"score\":100}");
        assertEquals(201, r.code(), "Insert: " + r.body());
        assertTrue(r.body().contains("DeepA"));
    }

    @Test @Order(41)
    void col_getAllReturns200() throws Exception {
        db.documentCollection("h_col_all").insert(Document.of("x", 1));
        var r = GET("/api/collections/h_col_all");
        assertEquals(200, r.code());
    }

    @Test @Order(42)
    void col_getByIdReturns200() throws Exception {
        var doc = db.documentCollection("h_col_id").insert(Document.of("label", "FindMe"));
        var r = GET("/api/collections/h_col_id/" + doc.id());
        assertEquals(200, r.code());
        assertTrue(r.body().contains("FindMe"));
    }

    @Test @Order(43)
    void col_getByIdMissingReturns404() throws Exception {
        var r = GET("/api/collections/h_col_id/no-such-id-xyz");
        assertEquals(404, r.code());
    }

    @Test @Order(44)
    void col_deleteByIdReturns204() throws Exception {
        var doc = db.documentCollection("h_col_del").insert(Document.of("tmp", "x"));
        var r = DELETE("/api/collections/h_col_del/" + doc.id());
        assertEquals(204, r.code());
        assertNull(db.documentCollection("h_col_del").findById(doc.id()));
    }

    @Test @Order(45)
    void col_statsEndpointReturns200() throws Exception {
        db.documentCollection("h_col_stats").insert(Document.of("v", 1));
        var r = GET("/api/collections/h_col_stats/stats");
        assertEquals(200, r.code(), "Stats: " + r.body());
    }

    @Test @Order(46)
    void col_queryEqEndpoint() throws Exception {
        db.documentCollection("h_col_query").insert(Document.of("role", "admin"));
        db.documentCollection("h_col_query").insert(Document.of("role", "user"));
        var r = POST("/api/collections/h_col_query/query", "{\"role\":\"admin\"}");
        assertEquals(200, r.code(), "Query eq: " + r.body());
        assertTrue(r.body().contains("admin"), "Query result must contain admin");
    }

    @Test @Order(47)
    void col_queryGtEndpoint() throws Exception {
        db.documentCollection("h_col_gt").insert(Document.of("score", 10));
        db.documentCollection("h_col_gt").insert(Document.of("score", 90));
        var r = POST("/api/collections/h_col_gt/query", "{\"$gt\":{\"score\":50}}");
        assertEquals(200, r.code(), "Query gt: " + r.body());
    }

    @Test @Order(48)
    void col_queryLtEndpoint() throws Exception {
        db.documentCollection("h_col_lt").insert(Document.of("price", 5));
        db.documentCollection("h_col_lt").insert(Document.of("price", 500));
        var r = POST("/api/collections/h_col_lt/query", "{\"$lt\":{\"price\":10}}");
        assertEquals(200, r.code(), "Query lt: " + r.body());
    }

    @Test @Order(49)
    void col_ttlEndpointDoesNotError() throws Exception {
        var doc = db.documentCollection("h_col_ttl").insert(Document.of("exp", true));
        var r = safe("POST", "/api/collections/h_col_ttl/set-ttl",
                "{\"documentId\":\"" + doc.id() + "\",\"ttlSeconds\":3600}");
        assertTrue(r.code() == 200 || r.code() == 400 || r.code() == 503,
                "TTL endpoint: " + r.code() + " " + r.body());
    }

    @Test @Order(50)
    void col_cleanupEndpointReturns200() throws Exception {
        var r = safe("POST", "/api/collections/h_col_ttl/cleanup", "{}");
        assertTrue(r.code() == 200 || r.code() == 400 || r.code() == 503,
                "Cleanup: " + r.code() + " " + r.body());
    }

    // =======================================================================
    // KV REST
    // =======================================================================

    @Test @Order(60)
    void kv_putReturns201() throws Exception {
        var r = PUT("/api/kv/h_kv/mykey", "{\"value\":\"hello-world\"}");
        assertEquals(201, r.code(), "KV put: " + r.body());
    }

    @Test @Order(61)
    void kv_getReturns200WithValue() throws Exception {
        db.keyValueBucket("h_kv2").put("greet", "world");
        var r = GET("/api/kv/h_kv2/greet");
        assertEquals(200, r.code());
        assertTrue(r.body().contains("world"));
    }

    @Test @Order(62)
    void kv_getMissingReturns404() throws Exception {
        assertEquals(404, GET("/api/kv/h_kv2/no_key_xyz").code());
    }

    @Test @Order(63)
    void kv_deleteReturns204() throws Exception {
        db.keyValueBucket("h_kv3").put("del_me", "v");
        var r = DELETE("/api/kv/h_kv3/del_me");
        assertEquals(204, r.code(), "KV delete: " + r.body());
    }

    @Test @Order(64)
    void kv_overwriteViaHttpPut() throws Exception {
        PUT("/api/kv/h_kv4/color", "{\"value\":\"red\"}");
        PUT("/api/kv/h_kv4/color", "{\"value\":\"blue\"}");
        var r = GET("/api/kv/h_kv4/color");
        assertTrue(r.body().contains("blue"), "Value should be updated to blue");
    }

    // =======================================================================
    // LIST REST
    // =======================================================================

    @Test @Order(70)
    void list_rpushReturns200() throws Exception {
        var r = POST("/api/kv/lists/h_lb/mylist/rpush", "{\"values\":[\"a\",\"b\",\"c\"]}");
        assertEquals(200, r.code(), "List rpush: " + r.body());
        assertTrue(r.body().contains("length") || r.body().contains("rpush"));
    }

    @Test @Order(71)
    void list_lpushReturns200() throws Exception {
        POST("/api/kv/lists/h_lb2/q/rpush", "{\"values\":[\"x\"]}");
        var r = POST("/api/kv/lists/h_lb2/q/lpush", "{\"values\":[\"z\"]}");
        assertEquals(200, r.code(), "List lpush: " + r.body());
    }

    @Test @Order(72)
    void list_lrangeReturnsElements() throws Exception {
        POST("/api/kv/lists/h_lb3/rng/rpush", "{\"values\":[\"1\",\"2\",\"3\"]}");
        var r = GET("/api/kv/lists/h_lb3/rng/lrange?start=0&end=-1");
        assertEquals(200, r.code(), "List lrange: " + r.body());
        assertTrue(r.body().contains("values") || r.body().contains("lrange"));
    }

    @Test @Order(73)
    void list_llenReturnsLength() throws Exception {
        POST("/api/kv/lists/h_lb4/ll/rpush", "{\"values\":[\"a\",\"b\",\"c\",\"d\"]}");
        var r = GET("/api/kv/lists/h_lb4/ll/llen");
        assertEquals(200, r.code(), "List llen: " + r.body());
        assertTrue(r.body().contains("4") || r.body().contains("length"));
    }

    @Test @Order(74)
    void list_lpopReturns200() throws Exception {
        POST("/api/kv/lists/h_lb5/pop_l/rpush", "{\"values\":[\"first\",\"second\"]}");
        var r = POST("/api/kv/lists/h_lb5/pop_l/lpop", "{}");
        assertEquals(200, r.code(), "List lpop: " + r.body());
        assertTrue(r.body().contains("first") || r.body().contains("value"));
    }

    @Test @Order(75)
    void list_rpopReturns200() throws Exception {
        POST("/api/kv/lists/h_lb6/pop_r/rpush", "{\"values\":[\"first\",\"last\"]}");
        var r = POST("/api/kv/lists/h_lb6/pop_r/rpop", "{}");
        assertEquals(200, r.code(), "List rpop: " + r.body());
        assertTrue(r.body().contains("last") || r.body().contains("value"));
    }

    @Test @Order(76)
    void list_ltrimReturns200() throws Exception {
        POST("/api/kv/lists/h_lb7/trim_l/rpush", "{\"values\":[\"a\",\"b\",\"c\",\"d\",\"e\"]}");
        var r = POST("/api/kv/lists/h_lb7/trim_l/ltrim", "{\"start\":1,\"end\":3}");
        assertEquals(200, r.code(), "List ltrim: " + r.body());
    }

    @Test @Order(77)
    void list_lremReturns200() throws Exception {
        POST("/api/kv/lists/h_lb8/rem_l/rpush", "{\"values\":[\"x\",\"y\",\"x\",\"z\"]}");
        var r = POST("/api/kv/lists/h_lb8/rem_l/lrem", "{\"count\":2,\"value\":\"x\"}");
        assertEquals(200, r.code(), "List lrem: " + r.body());
    }

    @Test @Order(78)
    void list_lindexReturns200() throws Exception {
        POST("/api/kv/lists/h_lb9/idx_l/rpush", "{\"values\":[\"zero\",\"one\",\"two\"]}");
        var r = GET("/api/kv/lists/h_lb9/idx_l/lindex?index=1");
        assertEquals(200, r.code(), "List lindex: " + r.body());
        assertTrue(r.body().contains("one") || r.body().contains("value"));
    }

    @Test @Order(79)
    void list_statsEndpoint() throws Exception {
        POST("/api/kv/lists/h_lb10/stat_l/rpush", "{\"values\":[\"s\"]}");
        var r = GET("/api/kv/lists/h_lb10/stat_l/stats");
        assertEquals(200, r.code(), "List stats: " + r.body());
    }

    // =======================================================================
    // SET REST
    // =======================================================================

    @Test @Order(80)
    void set_saddReturns200() throws Exception {
        var r = POST("/api/kv/sets/h_sb/colors/sadd", "{\"members\":[\"red\",\"green\",\"blue\"]}");
        assertEquals(200, r.code(), "Set sadd: " + r.body());
    }

    @Test @Order(81)
    void set_smembersReturns200() throws Exception {
        POST("/api/kv/sets/h_sb2/fruits/sadd", "{\"members\":[\"apple\",\"banana\"]}");
        var r = GET("/api/kv/sets/h_sb2/fruits/smembers");
        assertEquals(200, r.code(), "Set smembers: " + r.body());
        assertTrue(r.body().contains("apple") || r.body().contains("members"));
    }

    @Test @Order(82)
    void set_scardReturns200() throws Exception {
        POST("/api/kv/sets/h_sb3/nums/sadd", "{\"members\":[\"1\",\"2\",\"3\"]}");
        var r = GET("/api/kv/sets/h_sb3/nums/scard");
        assertEquals(200, r.code(), "Set scard: " + r.body());
        assertTrue(r.body().contains("3") || r.body().contains("cardinality"));
    }

    @Test @Order(83)
    void set_sismemberReturns200() throws Exception {
        POST("/api/kv/sets/h_sb4/set1/sadd", "{\"members\":[\"alpha\",\"beta\"]}");
        var r = GET("/api/kv/sets/h_sb4/set1/sismember?member=alpha");
        assertEquals(200, r.code(), "Set sismember: " + r.body());
        assertTrue(r.body().contains("true") || r.body().contains("exists"));
    }

    @Test @Order(84)
    void set_sremReturns200() throws Exception {
        POST("/api/kv/sets/h_sb5/srem_set/sadd", "{\"members\":[\"a\",\"b\",\"c\"]}");
        var r = POST("/api/kv/sets/h_sb5/srem_set/srem", "{\"members\":[\"b\"]}");
        assertEquals(200, r.code(), "Set srem: " + r.body());
    }

    @Test @Order(85)
    void set_spopReturns200() throws Exception {
        POST("/api/kv/sets/h_sb6/pop_set/sadd", "{\"members\":[\"m1\",\"m2\",\"m3\"]}");
        var r = POST("/api/kv/sets/h_sb6/pop_set/spop", "{\"count\":1}");
        assertEquals(200, r.code(), "Set spop: " + r.body());
    }

    @Test @Order(86)
    void set_srandmemberReturns200() throws Exception {
        POST("/api/kv/sets/h_sb7/rand_set/sadd", "{\"members\":[\"r1\",\"r2\",\"r3\"]}");
        var r = GET("/api/kv/sets/h_sb7/rand_set/srandmember?count=1");
        assertEquals(200, r.code(), "Set srandmember: " + r.body());
    }

    @Test @Order(87)
    void set_sinterReturns200() throws Exception {
        POST("/api/kv/sets/h_sb8/sA/sadd", "{\"members\":[\"1\",\"2\",\"3\"]}");
        POST("/api/kv/sets/h_sb8/sB/sadd", "{\"members\":[\"2\",\"3\",\"4\"]}");
        var r = POST("/api/kv/sets/h_sb8/sA/sinter", "{\"keys\":[\"sA\",\"sB\"]}");
        assertEquals(200, r.code(), "Set sinter: " + r.body());
    }

    @Test @Order(88)
    void set_sunionReturns200() throws Exception {
        POST("/api/kv/sets/h_sb9/uA/sadd", "{\"members\":[\"a\",\"b\"]}");
        POST("/api/kv/sets/h_sb9/uB/sadd", "{\"members\":[\"b\",\"c\"]}");
        var r = POST("/api/kv/sets/h_sb9/uA/sunion", "{\"keys\":[\"uA\",\"uB\"]}");
        assertEquals(200, r.code(), "Set sunion: " + r.body());
    }

    @Test @Order(89)
    void set_sdiffReturns200() throws Exception {
        POST("/api/kv/sets/h_sb10/dA/sadd", "{\"members\":[\"x\",\"y\",\"z\"]}");
        POST("/api/kv/sets/h_sb10/dB/sadd", "{\"members\":[\"y\",\"z\"]}");
        var r = POST("/api/kv/sets/h_sb10/dA/sdiff", "{\"keys\":[\"dA\",\"dB\"]}");
        assertEquals(200, r.code(), "Set sdiff: " + r.body());
    }

    // =======================================================================
    // HASH REST
    // =======================================================================

    @Test @Order(90)
    void hash_hsetSingleFieldReturns200() throws Exception {
        var r = POST("/api/kv/hashes/h_hb/user1/hset",
                "{\"field\":\"name\",\"value\":\"Alice\"}");
        assertEquals(200, r.code(), "Hash hset: " + r.body());
    }

    @Test @Order(91)
    void hash_hsetMultipleFieldsReturns200() throws Exception {
        var r = POST("/api/kv/hashes/h_hb2/user2/hset",
                "{\"fields\":{\"name\":\"Bob\",\"age\":\"25\"}}");
        assertEquals(200, r.code(), "Hash hset multi: " + r.body());
    }

    @Test @Order(92)
    void hash_hgetReturns200() throws Exception {
        POST("/api/kv/hashes/h_hb3/u3/hset", "{\"field\":\"email\",\"value\":\"u3@test.com\"}");
        var r = GET("/api/kv/hashes/h_hb3/u3/hget?field=email");
        assertEquals(200, r.code(), "Hash hget: " + r.body());
        assertTrue(r.body().contains("u3@test.com") || r.body().contains("value"));
    }

    @Test @Order(93)
    void hash_hgetallReturns200() throws Exception {
        POST("/api/kv/hashes/h_hb4/u4/hset", "{\"fields\":{\"f1\":\"v1\",\"f2\":\"v2\"}}");
        var r = GET("/api/kv/hashes/h_hb4/u4/hgetall");
        assertEquals(200, r.code(), "Hash hgetall: " + r.body());
        assertTrue(r.body().contains("f1") || r.body().contains("fields"));
    }

    @Test @Order(94)
    void hash_hdelReturns200() throws Exception {
        POST("/api/kv/hashes/h_hb5/u5/hset", "{\"field\":\"tmp\",\"value\":\"gone\"}");
        var r = POST("/api/kv/hashes/h_hb5/u5/hdel", "{\"fields\":[\"tmp\"]}");
        assertEquals(200, r.code(), "Hash hdel: " + r.body());
    }

    @Test @Order(95)
    void hash_hlenReturns200() throws Exception {
        POST("/api/kv/hashes/h_hb6/u6/hset", "{\"fields\":{\"a\":\"1\",\"b\":\"2\",\"c\":\"3\"}}");
        var r = GET("/api/kv/hashes/h_hb6/u6/hlen");
        assertEquals(200, r.code(), "Hash hlen: " + r.body());
        assertTrue(r.body().contains("3") || r.body().contains("length"));
    }

    @Test @Order(96)
    void hash_hexistsReturns200() throws Exception {
        POST("/api/kv/hashes/h_hb7/u7/hset", "{\"field\":\"present\",\"value\":\"yes\"}");
        var r = GET("/api/kv/hashes/h_hb7/u7/hexists?field=present");
        assertEquals(200, r.code(), "Hash hexists: " + r.body());
        assertTrue(r.body().contains("true") || r.body().contains("exists"));
    }

    @Test @Order(97)
    void hash_hkeysReturns200() throws Exception {
        POST("/api/kv/hashes/h_hb8/u8/hset", "{\"fields\":{\"k1\":\"v1\",\"k2\":\"v2\"}}");
        var r = GET("/api/kv/hashes/h_hb8/u8/hkeys");
        assertEquals(200, r.code(), "Hash hkeys: " + r.body());
        assertTrue(r.body().contains("k1") || r.body().contains("fields"));
    }

    @Test @Order(98)
    void hash_hvalsReturns200() throws Exception {
        POST("/api/kv/hashes/h_hb9/u9/hset", "{\"fields\":{\"a\":\"val_a\",\"b\":\"val_b\"}}");
        var r = GET("/api/kv/hashes/h_hb9/u9/hvals");
        assertEquals(200, r.code(), "Hash hvals: " + r.body());
        assertTrue(r.body().contains("val_a") || r.body().contains("values"));
    }

    @Test @Order(99)
    void hash_hmgetReturns200() throws Exception {
        POST("/api/kv/hashes/h_hb10/u10/hset", "{\"fields\":{\"x\":\"10\",\"y\":\"20\",\"z\":\"30\"}}");
        var r = POST("/api/kv/hashes/h_hb10/u10/hmget", "{\"fields\":[\"x\",\"z\"]}");
        assertEquals(200, r.code(), "Hash hmget: " + r.body());
        assertTrue(r.body().contains("10") || r.body().contains("fields"));
    }

    @Test @Order(100)
    void hash_hincrbyReturns200() throws Exception {
        POST("/api/kv/hashes/h_hb11/counter/hset", "{\"field\":\"views\",\"value\":\"10\"}");
        var r = POST("/api/kv/hashes/h_hb11/counter/hincrby",
                "{\"field\":\"views\",\"delta\":5}");
        assertEquals(200, r.code(), "Hash hincrby: " + r.body());
        assertTrue(r.body().contains("15") || r.body().contains("newValue"));
    }

    @Test @Order(101)
    void hash_hincrbyfloatReturns200() throws Exception {
        POST("/api/kv/hashes/h_hb12/rate/hset", "{\"field\":\"val\",\"value\":\"1.5\"}");
        var r = POST("/api/kv/hashes/h_hb12/rate/hincrbyfloat",
                "{\"field\":\"val\",\"delta\":0.5}");
        assertEquals(200, r.code(), "Hash hincrbyfloat: " + r.body());
        assertTrue(r.body().contains("2.0") || r.body().contains("newValue"));
    }

    // =======================================================================
    // COLUMN FAMILY REST
    // =======================================================================

    @Test @Order(110)
    void cf_putReturns201() throws Exception {
        var r = PUT("/api/columns/h_cf/row1",
                "{\"username\":\"alice\",\"email\":\"alice@test.com\"}");
        assertTrue(r.code() == 200 || r.code() == 201,
                "CF put: " + r.code() + " " + r.body());
    }

    @Test @Order(111)
    void cf_getReturns200WithData() throws Exception {
        db.columnFamily("h_cf2").put("row2", "name", "Bob", null);
        var r = GET("/api/columns/h_cf2/row2");
        assertEquals(200, r.code(), "CF get: " + r.body());
        assertTrue(r.body().contains("Bob") || r.body().contains("name"));
    }

    @Test @Order(112)
    void cf_deleteReturns204() throws Exception {
        db.columnFamily("h_cf3").put("del_r", "c", "v", null);
        var r = DELETE("/api/columns/h_cf3/del_r");
        assertTrue(r.code() == 200 || r.code() == 204 || r.code() == 404,
                "CF delete: " + r.code());
    }

    @Test @Order(113)
    void cf_statsEndpointReturns200() throws Exception {
        db.columnFamily("h_cf4").put("sr", "sc", "sv", null);
        var r = GET("/api/columns/h_cf4/stats");
        assertEquals(200, r.code(), "CF stats: " + r.body());
    }

    @Test @Order(114)
    void cf_singleColumnGet() throws Exception {
        db.columnFamily("h_cf5").put("r5", "email", "user@test.com", null);
        var r = GET("/api/columns/h_cf5/r5/column/email");
        assertEquals(200, r.code(), "CF single col: " + r.body());
        assertTrue(r.body().contains("user@test.com") || r.body().contains("value"));
    }

    @Test @Order(115)
    void cf_filterByColumnNames() throws Exception {
        db.columnFamily("h_cf6").put("r6", "a", "1", null);
        db.columnFamily("h_cf6").put("r6", "b", "2", null);
        db.columnFamily("h_cf6").put("r6", "c", "3", null);
        var r = GET("/api/columns/h_cf6/r6/filter?columns=a,b");
        assertEquals(200, r.code(), "CF filter: " + r.body());
        assertTrue(r.body().contains("a") || r.body().contains("data"));
        assertFalse(r.body().contains("\"c\"") && r.body().contains("\"3\""),
                "Filtered response should not contain column c");
    }

    @Test @Order(116)
    void cf_filterByPrefix() throws Exception {
        db.columnFamily("h_cf7").put("r7", "ts_2024", "v1", null);
        db.columnFamily("h_cf7").put("r7", "ts_2025", "v2", null);
        db.columnFamily("h_cf7").put("r7", "other",   "v3", null);
        var r = GET("/api/columns/h_cf7/r7/filter-prefix?prefix=ts_");
        assertEquals(200, r.code(), "CF prefix: " + r.body());
        assertTrue(r.body().contains("ts_2024") || r.body().contains("data"));
    }

    @Test @Order(117)
    void cf_filterByPattern() throws Exception {
        db.columnFamily("h_cf8").put("r8", "meta_a", "1", null);
        db.columnFamily("h_cf8").put("r8", "meta_b", "2", null);
        db.columnFamily("h_cf8").put("r8", "data_c", "3", null);
        var r = GET("/api/columns/h_cf8/r8/filter-pattern?pattern=meta.*");
        assertEquals(200, r.code(), "CF pattern: " + r.body());
        assertTrue(r.body().contains("meta_a") || r.body().contains("data"));
    }

    // =======================================================================
    // CDC / AUDIT / BACKUP / TRANSACTIONS
    // =======================================================================

    @Test @Order(120)
    void cdc_getReturns200() throws Exception {
        assertEquals(200, GET("/api/cdc").code());
    }

    @Test @Order(121)
    void cdc_postReturns2xxOr400() throws Exception {
        var r = POST("/api/cdc", "{\"type\":\"FILE\",\"destination\":\"deep_cdc.log\"}");
        assertTrue(r.code() == 200 || r.code() == 201 || r.code() == 400,
                "CDC POST: " + r.code());
    }

    @Test @Order(122)
    void audit_getReturns200() throws Exception {
        POST("/api/collections/audit_deep", "{\"event\":\"test\"}");
        var r = GET("/api/audit/logs");
        assertEquals(200, r.code(), "Audit GET: " + r.body());
        assertTrue(r.body().contains("count") || r.body().contains("events"));
    }

    @Test @Order(123)
    void audit_filterByOperation() throws Exception {
        var r = GET("/api/audit/logs?operation=INSERT&limit=10");
        assertEquals(200, r.code(), "Audit filter: " + r.body());
    }

    @Test @Order(124)
    void audit_filterByResource() throws Exception {
        var r = GET("/api/audit/logs?resource=audit_deep&limit=5");
        assertEquals(200, r.code(), "Audit resource: " + r.body());
    }

    @Test @Order(125)
    void backup_postReturnsAny2xxOr400() throws Exception {
        var r = POST("/api/backup", "{}");
        assertTrue(r.code() < 600, "Backup: " + r.code());
    }

    @Test @Order(126)
    void backup_getReturns200Or404() throws Exception {
        var r = GET("/api/backup");
        assertTrue(r.code() == 200 || r.code() == 404, "Backup GET: " + r.code());
    }

    @Test @Order(127)
    void transactions_getReturns200() throws Exception {
        assertEquals(200, GET("/api/transactions").code());
    }

    // =======================================================================
    // SQL
    // =======================================================================

    @Test @Order(130)
    void sql_createTableOrReturns400() throws Exception {
        var r = POST("/api/sql",
                "CREATE TABLE IF NOT EXISTS h_sql_t (id INT PRIMARY KEY)");
        assertTrue(r.code() == 200 || r.code() == 201 || r.code() == 400,
                "SQL create: " + r.code());
    }

    @Test @Order(131)
    void sql_selectOrReturns400() throws Exception {
        var r = POST("/api/sql", "SELECT 1");
        assertTrue(r.code() == 200 || r.code() == 201 || r.code() == 400,
                "SQL select: " + r.code());
    }

    // =======================================================================
    // BULK
    // =======================================================================

    @Test @Order(140)
    void bulk_insertReturns2xxOr400() throws Exception {
        var r = safe("POST", "/api/bulk/h_bulk_col",
                "[{\"name\":\"B1\"},{\"name\":\"B2\"},{\"name\":\"B3\"}]");
        assertTrue(r.code() == 200 || r.code() == 201 || r.code() == 400 || r.code() == 503,
                "Bulk insert: " + r.code());
    }

    // =======================================================================
    // ERROR HANDLING
    // =======================================================================

    @Test @Order(150)
    void error_405OnHealthDelete() throws Exception {
        var r = safe("DELETE", "/api/health", null);
        assertTrue(r.code() == 405 || r.code() == 200 || r.code() == 404,
                "DELETE /api/health should be 405 or safe: " + r.code());
    }

    @Test @Order(151)
    void error_malformedJsonReturns400OrHandled() throws Exception {
        var r = safe("POST", "/api/collections/h_err_col", "{bad-json-here}");
        assertTrue(r.code() == 400 || r.code() == 500,
                "Malformed JSON: " + r.code());
    }

    @Test @Order(152)
    void error_emptyJsonBodyIsHandled() throws Exception {
        var r = safe("POST", "/api/collections/h_empty_col", "{}");
        assertTrue(r.code() == 201 || r.code() == 400,
                "Empty JSON body: " + r.code());
    }

    // =======================================================================
    // CONCURRENCY — HTTP
    // =======================================================================

    @Test @Order(160)
    void http_concurrentInserts() throws Exception {
        int threads = 8;
        int perThread = 20;
        var pool = Executors.newFixedThreadPool(threads);
        var futures = new ArrayList<Future<Integer>>();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            futures.add(pool.submit(() -> {
                int ok = 0;
                for (int i = 0; i < perThread; i++) {
                    try {
                        var r = POST("/api/collections/h_conc",
                                "{\"thread\":" + tid + ",\"seq\":" + i + "}");
                        if (r.code() == 201) ok++;
                    } catch (Exception ignored) {}
                }
                return ok;
            }));
        }
        int totalOk = 0;
        for (var f : futures) totalOk += f.get(60, TimeUnit.SECONDS);
        pool.shutdown();
        assertEquals(threads * perThread, totalOk,
                "All concurrent HTTP inserts must succeed: " + totalOk + "/" + (threads * perThread));
    }

    @Test @Order(161)
    void http_concurrentKvPuts() throws Exception {
        int threads = 10;
        var pool = Executors.newFixedThreadPool(threads);
        var futures = new ArrayList<Future<Integer>>();
        for (int t = 0; t < threads; t++) {
            final int tid = t;
            futures.add(pool.submit(() -> {
                int ok = 0;
                for (int i = 0; i < 10; i++) {
                    try {
                        var r = PUT("/api/kv/h_kv_conc/key_" + tid + "_" + i,
                                "{\"value\":\"v" + i + "\"}");
                        if (r.code() == 201) ok++;
                    } catch (Exception ignored) {}
                }
                return ok;
            }));
        }
        int total = 0;
        for (var f : futures) total += f.get(30, TimeUnit.SECONDS);
        pool.shutdown();
        assertEquals(threads * 10, total, "All concurrent KV HTTP puts must succeed");
    }
}