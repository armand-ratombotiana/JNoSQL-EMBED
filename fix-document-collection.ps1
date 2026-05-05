#!/usr/bin/env pwsh
# Fix DocumentCollection.java - remove structured concurrency

$file = "src\main\java\org\junify\db\nosql\document\DocumentCollection.java"
$content = Get-Content $file -Raw

# Remove the structured concurrency import
$content = $content -replace 'import java\.util\.concurrent\.StructuredTaskScope;\s*', ''

# Replace the insertAll method with simple sequential version
$oldMethod = @'
    /**
     * Java 25: Batch insert with structured concurrency.
     * Uses virtual threads for parallel insertion.
     */
    public List<Document> insertAll(List<Document> docs) {
        // For small batches, use sequential insert
        if (docs.size() < 10) {
            return docs.stream().map(this::insert).collect(Collectors.toList());
        }

        // Java 25: StructuredTaskScope for parallel batch operations
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            var futures = new ArrayList<StructuredTaskScope.Subtask<Document>>();

            // Fork each insert as a subtask
            for (var doc : docs) {
                futures.add(scope.fork(() -> insert(doc)));
            }

            scope.join();
            scope.throwIfFailed();

            // Collect results
            var results = new ArrayList<Document>(docs.size());
            for (var future : futures) {
                if (future.state() == StructuredTaskScope.Subtask.State.SUCCESS) {
                    results.add(future.get());
                }
            }
            return results;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Batch insert interrupted", e);
        } catch (Exception e) {
            throw new RuntimeException("Batch insert failed", e);
        }
    }
'@

$newMethod = @'
    /**
     * Batch insert documents sequentially.
     */
    public List<Document> insertAll(List<Document> docs) {
        return docs.stream().map(this::insert).collect(Collectors.toList());
    }
'@

$content = $content -replace [regex]::Escape($oldMethod), $newMethod
$content | Set-Content $file -NoNewline

Write-Host "Fixed DocumentCollection.java"
