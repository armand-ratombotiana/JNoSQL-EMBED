// JunifyDB Authentication Setup Script
// Creates default admin user for the database console

import java.sql.*;

public class SetupAdminUser {
    private static final String DB_URL = "jdbc:h2:file:./data/embeddb;MODE=MySQL";
    private static final String DB_USER = "sa";
    private static final String DB_PASS = "sa";
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== JunifyDB Admin User Setup ===");
        
        // Load H2 driver
        Class.forName("org.h2.Driver");
        
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            // Check if admin user already exists
            PreparedStatement checkStmt = conn.prepareStatement(
                "SELECT COUNT(*) FROM db_users WHERE username = ?"
            );
            checkStmt.setString(1, "admin");
            ResultSet rs = checkStmt.executeQuery();
            rs.next();
            
            if (rs.getInt(1) > 0) {
                System.out.println("Admin user already exists.");
                
                // Reset admin password
                String salt = java.util.UUID.randomUUID().toString().substring(0, 8);
                String hash = hashPassword("admin123", salt);
                
                PreparedStatement updateStmt = conn.prepareStatement(
                    "UPDATE db_users SET password_hash = ?, salt = ?, role = ?, enabled = TRUE WHERE username = ?"
                );
                updateStmt.setString(1, hash);
                updateStmt.setString(2, salt);
                updateStmt.setString(3, "ADMIN");
                updateStmt.setString(4, "admin");
                int updated = updateStmt.executeUpdate();
                
                System.out.println("Admin password reset successfully.");
            } else {
                // Create admin user
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
                int inserted = insertStmt.executeUpdate();
                
                System.out.println("Admin user created successfully.");
            }
            
            // Display credentials
            System.out.println("\n=== Default Admin Credentials ===");
            System.out.println("Username: admin");
            System.out.println("Password: admin123");
            System.out.println("Role: ADMIN");
            System.out.println("\nIMPORTANT: Change the password after first login!");
            System.out.println("===============================\n");
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
