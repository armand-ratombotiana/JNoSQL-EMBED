import re

content = open('c:/Users/jratombo-adm/Desktop/JNoSQL-EMBED/src/main/java/org/junify/db/console/http/JunifyDBServer.java', 'r', encoding='utf-8').read()

# Find where to insert the methods (before startHttpsServer)
insert_marker = '    private void startHttpsServer() {'

methods_to_add = '''
    /**
     * Initialize default admin user if not exists.
     */
    private void initializeAdminUser() {
        try {
            // Check if db_users table exists
            var tableCheck = db.h2Engine().executeSql(
                "SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'DB_USERS'"
            );
            boolean tableExists = false;
            if (tableCheck.success() && tableCheck.rows() != null && !tableCheck.rows().isEmpty()) {
                var cnt = tableCheck.rows().get(0).get("CNT");
                tableExists = cnt instanceof Number && ((Number) cnt).intValue() > 0;
            }

            if (!tableExists) {
                System.out.println("[Auth] db_users table not yet created - will be created by UserManager on first access");
                return;
            }

            // Check if admin user exists
            var checkResult = db.h2Engine().executeSql(
                "SELECT COUNT(*) as cnt FROM db_users WHERE username = 'admin'"
            );
            boolean adminExists = false;
            if (checkResult.success() && checkResult.rows() != null && !checkResult.rows().isEmpty()) {
                var cnt = checkResult.rows().get(0).get("CNT");
                adminExists = cnt instanceof Number && ((Number) cnt).intValue() > 0;
            }

            if (!adminExists) {
                // Create default admin user
                String salt = java.util.UUID.randomUUID().toString().substring(0, 8);
                String password = "admin123";
                String hash = hashPassword(password, salt);

                var insertResult = db.h2Engine().executeSql(
                    "INSERT INTO db_users (username, password_hash, salt, role, created_at, enabled) VALUES (?, ?, ?, ?, ?, ?)",
                    "admin", hash, salt, "ADMIN", System.currentTimeMillis(), true
                );

                if (insertResult.success()) {
                    System.out.println("=================================================");
                    System.out.println("  DEFAULT ADMIN USER CREATED");
                    System.out.println("=================================================");
                    System.out.println("  Username: admin");
                    System.out.println("  Password: admin123");
                    System.out.println("  Role: ADMIN");
                    System.out.println("=================================================");
                    System.out.println("  IMPORTANT: Change password after first login!");
                    System.out.println("=================================================");
                }
            } else {
                System.out.println("[Auth] Admin user already exists");
            }
        } catch (Exception e) {
            System.err.println("[Auth] Failed to initialize admin user: " + e.getMessage());
        }
    }

    /**
     * Hash password using SHA-256 with salt.
     */
    private String hashPassword(String password, String salt) {
        try {
            var md = java.security.MessageDigest.getInstance("SHA-256");
            var bytes = md.digest((password + salt).getBytes());
            var sb = new StringBuilder();
            for (var b : bytes) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException("SHA-256 not available", e);
        }
    }

''' + insert_marker

content = content.replace(insert_marker, methods_to_add)

open('c:/Users/jratombo-adm/Desktop/JNoSQL-EMBED/src/main/java/org/junify/db/console/http/JunifyDBServer.java', 'w', encoding='utf-8').write(content)
print('Methods added successfully')
