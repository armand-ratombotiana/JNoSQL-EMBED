public class DebugTest {
    public static void main(String[] args) {
        String sql = "SELECT * FROM orders";
        System.out.println("SQL: " + sql);
        System.out.println("Upper: " + sql.toUpperCase());
        System.out.println("Contains SELECT: " + sql.toUpperCase().contains("SELECT"));
        System.out.println("Contains WHERE: " + sql.toUpperCase().contains("WHERE"));
        System.out.println("Contains LIKE: " + sql.toUpperCase().contains("LIKE"));
        System.out.println("Contains OR: " + sql.toUpperCase().contains("OR"));
        
        boolean result = sql.toUpperCase().contains("SELECT") && sql.toUpperCase().contains("WHERE");
        System.out.println("First if condition (SELECT && WHERE): " + result);
        
        System.out.println("\nExpected isOptimized result: true (should skip all if blocks and return true)");
    }
}
