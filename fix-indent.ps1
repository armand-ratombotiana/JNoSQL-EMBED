#!/usr/bin/env pwsh
# Fix JunifyDBServer.java indentation

$file = "src\main\java\org\junify\db\console\http\JunifyDBServer.java"
$content = Get-Content $file -Raw

# The file has a section from line ~908 to end where everything is indented with 4 extra spaces
# We need to fix this section

$lines = $content -split "`r?`n"
$output = @()
$fixMode = $false

for ($i = 0; $i -lt $lines.Count; $i++) {
    $line = $lines[$i]
    
    # Start fixing at TablesHandler
    if ($line -match '^\s{8}private class TablesHandler') {
        $fixMode = $true
    }
    
    if ($fixMode) {
        # Remove 4 spaces from the beginning if line starts with 8+ spaces
        if ($line -match '^ {8,}') {
            $line = $line.Substring(4)
        }
    }
    
    $output += $line
}

$output -join "`n" | Set-Content $file -NoNewline
Write-Host "Fixed indentation in $file"
