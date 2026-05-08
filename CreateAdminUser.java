import java.sql.*;

public class CreateAdminUser {
    private static final String DB_URL = "jdbc:h2:file:./data/embeddb;MODE=MySQL;DB_CLOSE_DELAY=-1";
    private static final String DB_USER = "sa";
    private static final String DB_PASS = "sa";
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== Creating Admin User ===");
        
        Class.forName("org.h2.Driver");
        
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            // Check if admin exists
            PreparedStatement checkStmt = conn.prepareStatement(
                "SELECT COUNT(*) as cnt FROM db_users WHERE username = 'admin'"
            );
            ResultSet rs = checkStmt.executeQuery();
            rs.next();
            
            if (rs.getInt("cnt") > 0) {
                System.out.println("Admin user already exists!");
                
                // Reset password
                String salt = java.util.UUID.randomUUID().toString().substring(0, 8);
                String hash = hashPassword("admin123", salt);
                
                PreparedStatement updateStmt = conn.prepareStatement(
                    "UPDATE db_users SET password_hash = ?, salt = ? WHERE username = 'admin'"
                );
                updateStmt.setString(1, hash);
                updateStmt.setString(2, salt);
                updateStmt.executeUpdate();
                
                System.out.println("Password reset to: admin123");
            } else {
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
                
                System.out.println("Admin user created!");
            }
            
            System.out.println("\n=== Credentials ===");
            System.out.println("Username: admin");
            System.out.println("Password: admin123");
            System.out.println("=====================\n");
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
