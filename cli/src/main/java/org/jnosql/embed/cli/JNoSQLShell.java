package org.junify.db.integration.standalone;

import org.junify.db.JunifyDB;
import org.junify.db.config.JunifyDBConfig;
import org.junify.db.document.Document;
import org.junify.db.document.DocumentCollection;
import org.junify.db.document.Query;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;

public class JunifyDBShell {

    private final JunifyDB db;
    private final BufferedReader reader;

    public JunifyDBShell(JunifyDB db) {
        this.db = db;
        this.reader = new BufferedReader(new InputStreamReader(System.in));
    }

    public void start() throws IOException {
        System.out.println("junify-EMBED Shell v1.0");
        System.out.println("Type 'help' for commands, 'exit' to quit");

        while (true) {
            System.out.print("JUNIFYDB> ");
            String line = reader.readLine();
            if (line == null) break; // EOF

            line = line.trim();
            if (line.isEmpty()) continue;

            if (line.equalsIgnoreCase("exit")) {
                break;
            } else if (line.equalsIgnoreCase("help")) {
                printHelp();
            } else if (line.startsWith("use ")) {
                useDatabase(line.substring(4).trim());
            } else if (line.startsWith("insert ")) {
                insertDocument(line.substring(7).trim());
            } else if (line.startsWith("find ")) {
                findDocuments(line.substring(5).trim());
            } else if (line.startsWith("count")) {
                showCount();
            } else if (line.startsWith("stats")) {
                showStats();
            } else if (line.startsWith("quit")) {
                break;
            } else {
                System.out.println("Unknown command: " + line);
                System.out.println("Type 'help' for available commands");
            }
        }
    }

    private void printHelp() {
        System.out.println("Available commands:");
        System.out.println("  help          - Show this help");
        System.out.println("  use <name>    - Use/create collection");
        System.out.println("  insert <json> - Insert document (JSON format)");
        System.out.println("  find <query>  - Find documents");
        System.out.println("  count         - Show document count");
        System.out.println("  stats         - Show collection stats");
        System.out.println("  exit/quit     - Exit shell");
    }

    private void useDatabase(String collectionName) {
        DocumentCollection collection = db.documentCollection(collectionName);
        System.out.println("Using collection: " + collectionName);
    }

    private void insertDocument(String json) {
        try {
            Document doc = Document.fromJson(json);
            DocumentCollection collection = db.documentCollection("default");
            Document saved = collection.insert(doc);
            System.out.println("Inserted with id: " + saved.id());
        } catch (Exception e) {
            System.out.println("Error inserting document: " + e.getMessage());
        }
    }

    private void findDocuments(String queryStr) {
        try {
            DocumentCollection collection = db.documentCollection("default");
            Query query;
            
            if (queryStr.isEmpty() || queryStr.equalsIgnoreCase("all")) {
                query = Query.all();
            } else if (queryStr.startsWith("{")) {
                // Simple JSON query parsing (basic implementation)
                query = Query.all(); // Would parse JSON in real implementation
            } else {
                // Treat as key-value for simple search
                String[] parts = queryStr.split("=", 2);
                if (parts.length == 2) {
                    query = Query.eq(parts[0].trim(), parts[1].trim());
                } else {
                    query = Query.all();
                }
            }
            
            var results = collection.find(query);
            System.out.println("Found " + results.size() + " documents:");
            for (Document doc : results) {
                System.out.println("  " + doc);
            }
        } catch (Exception e) {
            System.out.println("Error executing query: " + e.getMessage());
        }
    }

    private void showCount() {
        DocumentCollection collection = db.documentCollection("default");
        long count = collection.count();
        System.out.println("Document count: " + count);
    }

    private void showStats() {
        DocumentCollection collection = db.documentCollection("default");
        var stats = collection.stats();
        System.out.println("Collection stats:");
        for (var entry : stats.entrySet()) {
            System.out.println("  " + entry.getKey() + ": " + entry.getValue());
        }
    }

    public static void main(String[] args) throws IOException {
        JunifyDB db = JUNIFYDB.embed().build();
        try {
            new JunifyDBShell(db).start();
        } finally {
            db.close();
        }
    }
}
