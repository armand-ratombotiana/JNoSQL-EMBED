// HTTP-based admin user creator for JunifyDB
import java.net.http.*;
import java.net.URI;
import java.sql.*;

public class SetupAdminViaHTTP {
    private static final String API_KEY = "hYXuECpj4dM28vf3En47ar2KaA1FPMLVNrmAYYSAoFM";
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== Setting Up JunifyDB Admin User ===\n");
        
        HttpClient client = HttpClient.newHttpClient();
        
        // First, trigger UserManager to create tables by accessing an endpoint that uses it
        // We'll use a simple SQL query that will force the engine to initialize
        String sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'DB_USERS'";
        
        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create("http://localhost:8080/api/sql"))
            .header("Content-Type", "application/json")
            .header("X-API-Key", API_KEY)
            .POST(HttpRequest.BodyPublishers.ofString("{\"sql\":\"" + sql + "\"}"))
            .build();
        
        try {
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println("Server response: " + response.body());
            
            // Now create admin user using direct database access
            System.out.println("\nNow creating admin user via direct DB connection...");
            
            // Stop server first
            System.out.println("Stopping server temporarily...");
            ProcessBuilder pb = new ProcessBuilder("taskkill", "/F", "/IM", "java.exe");
            pb.start().waitFor();
            Thread.sleep(2000);
            
            // Create admin
            Class.forName("org.h2.Driver");
            Connection conn = DriverManager.getConnection("jdbc:h2:file:./data/embeddb;MODE=MySQL", "sa", "sa");
            
            // Create table
            conn.createStatement().execute(
                "CREATE TABLE IF NOT EXISTS db_users (" +
                "username VARCHAR(255) PRIMARY KEY, " +
                "password_hash VARCHAR(512), " +
                "salt VARCHAR(128), " +
                "role VARCHAR(50) DEFAULT 'USER', " +
                "created_at BIGINT, " +
                "last_login BIGINT, " +
                "enabled BOOLEAN DEFAULT TRUE)"
            );
            
            // Check and create/update admin
            PreparedStatement checkStmt = conn.prepareStatement("SELECT COUNT(*) as cnt FROM db_users WHERE username = 'admin'");
            ResultSet rs = checkStmt.executeQuery();
            rs.next();
            
            String salt = java.util.UUID.randomUUID().toString().substring(0, 8);
            String hash = hashPassword("admin123", salt);
            
            if (rs.getInt("cnt") > 0) {
                PreparedStatement updateStmt = conn.prepareStatement("UPDATE db_users SET password_hash = ?, salt = ? WHERE username = 'admin'");
                updateStmt.setString(1, hash);
                updateStmt.setString(2, salt);
                updateStmt.executeUpdate();
                System.out.println("✓ Admin password reset");
            } else {
                PreparedStatement insertStmt = conn.prepareStatement(
                    "INSERT INTO db_users (username, password_hash, salt, role, created_at, enabled) VALUES (?, ?, ?, ?, ?, ?)");
                insertStmt.setString(1, "admin");
                insertStmt.setString(2, hash);
                insertStmt.setString(3, salt);
                insertStmt.setString(4, "ADMIN");
                insertStmt.setLong(5, System.currentTimeMillis());
                insertStmt.setBoolean(6, true);
                insertStmt.executeUpdate();
                System.out.println("✓ Admin user created");
            }
            
            conn.close();
            
            System.out.println("\n===========================================");
            System.out.println("  ✓ ADMIN USER READY");
            System.out.println("===========================================");
            System.out.println("  Username: admin");
            System.out.println("  Password: admin123");
            System.out.println("===========================================");
            System.out.println("\nStarting server...");
            
            // Restart server
            new ProcessBuilder("cmd", "/c", "start", "/B", "java", "-cp", "target/classes;target/dep/*", 
                "org.junify.db.JunifyDB", "--port", "8080", "--engine", "H2").start();
            
            Thread.sleep(5000);
            System.out.println("✓ Server restarted");
            System.out.println("\nLogin at: http://localhost:8080/login.html\n");
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static String hashPassword(String password, String salt) throws Exception {
        java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-256");
        byte[] digest = md.digest((password + salt).getBytes());
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
