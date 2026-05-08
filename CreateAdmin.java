// Create admin user for JunifyDB authentication
import java.sql.*;
import java.security.MessageDigest;

public class CreateAdmin {
    public static void main(String[] args) throws Exception {
        System.out.println("=== Creating JunifyDB Admin User ===\n");
        
        // Use file-based connection (server must be stopped)
        String dbUrl = "jdbc:h2:file:./data/embeddb;MODE=MySQL";
        
        try {
            Class.forName("org.h2.Driver");
            Connection conn = DriverManager.getConnection(dbUrl, "sa", "sa");
            
            // First check what tables exist
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet tables = meta.getTables(null, null, "DB_USERS", null);
            boolean tableExists = tables.next();
            tables.close();
            
            if (!tableExists) {
                System.out.println("Creating db_users table...");
                conn.createStatement().execute(
                    "CREATE TABLE db_users (" +
                    "username VARCHAR(255) PRIMARY KEY, " +
                    "password_hash VARCHAR(512), " +
                    "salt VARCHAR(128), " +
                    "role VARCHAR(50), " +
                    "created_at BIGINT, " +
                    "last_login BIGINT, " +
                    "enabled BOOLEAN)"
                );
            }
            System.out.println("✓ db_users table created/verified");
            
            // Check if admin exists
            PreparedStatement checkStmt = conn.prepareStatement(
                "SELECT COUNT(*) as cnt FROM db_users WHERE username = 'admin'"
            );
            ResultSet rs = checkStmt.executeQuery();
            rs.next();
            
            if (rs.getInt("cnt") > 0) {
                System.out.println("✓ Admin user already exists - resetting password");
                
                String salt = java.util.UUID.randomUUID().toString().substring(0, 8);
                String hash = hashPassword("admin123", salt);
                
                PreparedStatement updateStmt = conn.prepareStatement(
                    "UPDATE db_users SET password_hash = ?, salt = ? WHERE username = 'admin'"
                );
                updateStmt.setString(1, hash);
                updateStmt.setString(2, salt);
                updateStmt.executeUpdate();
            } else {
                System.out.println("✓ Creating admin user");
                
                String salt = java.util.UUID.randomUUID().toString().substring(0, 8);
                String hash = hashPassword("admin123", salt);
                
                PreparedStatement insertStmt = conn.prepareStatement(
                    "INSERT INTO db_users (username, password_hash, salt, role, created_at, enabled) VALUES (?, ?, ?, ?, ?, ?)"
                );
                insertStmt.setString(1, "admin");
                insertStmt.setString(2, hash);
                insertStmt.setString(3, salt);
                insertStmt.setString(4, "ADMIN");
                insertStmt.setLong(5, System.currentTimeMillis());
                insertStmt.setBoolean(6, true);
                insertStmt.executeUpdate();
            }
            
            conn.close();
            
            System.out.println("\n===========================================");
            System.out.println("  ADMIN USER READY");
            System.out.println("===========================================");
            System.out.println("  Username: admin");
            System.out.println("  Password: admin123");
            System.out.println("  Role: ADMIN");
            System.out.println("===========================================");
            System.out.println("  Login at: http://localhost:8080/login.html");
            System.out.println("===========================================\n");
            
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println("\nMake sure the JunifyDB server is running on port 8080");
        }
    }
    
    private static String hashPassword(String password, String salt) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] digest = md.digest((password + salt).getBytes());
        StringBuilder sb = new StringBuilder();
        for (byte b : digest) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
