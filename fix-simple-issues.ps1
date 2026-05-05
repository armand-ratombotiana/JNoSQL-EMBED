#!/usr/bin/env pwsh
# Fix all simple type issues in one pass

$files = @(
    "src/main/java/org/junify/db/kv/KeyValueBucket.java",
    "src/main/java/org/junify/db/console/JunifyConsole.java",
    "src/main/java/org/junify/db/sql/executor/SqlExecutor.java",
    "src/main/java/org/junify/db/sql/parser/SqlParser.java"
)

foreach ($file in $files) {
    if (Test-Path $file) {
        $content = Get-Content $file -Raw
        $original = $content
        
        # Fix EventBus imports
        $content = $content -replace 'import org\.junify\.db\.event\.EventBus', 'import org.junify.db.core.event.EventBus'
        
        # Fix Transaction imports
        $content = $content -replace 'import org\.junify\.db\.transaction\.mvcc\.Transaction', 'import org.junify.db.transaction.Transaction'
        
        # Fix ArrayList to List
        $content = $content -replace 'java\.util\.ArrayList<org\.junify\.db\.nosql\.document\.Document>', 'java.util.List<org.junify.db.nosql.document.Document>'
        
        # Fix SqlParser Token to String
        $content = $content -replace 'var table = consumeIdentifier\(\);', 'var table = consumeIdentifier().value();'
        $content = $content -replace 'var col = consumeIdentifier\(\);', 'var col = consumeIdentifier().value();'
        
        if ($content -ne $original) {
            Set-Content $file $content -NoNewline
            Write-Host "Fixed: $file"
        }
    }
}

Write-Host "Done!"
