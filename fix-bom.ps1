#!/usr/bin/env pwsh
# Remove UTF-8 BOM from all Java files

$files = Get-ChildItem -Path "src\main\java","src\test\java" -Recurse -Filter "*.java"
$bom = [byte[]]@(0xEF, 0xBB, 0xBF)
$fixed = 0

foreach ($file in $files) {
    $bytes = [System.IO.File]::ReadAllBytes($file.FullName)
    if ($bytes.Length -ge 3 -and $bytes[0] -eq $bom[0] -and $bytes[1] -eq $bom[1] -and $bytes[2] -eq $bom[2]) {
        # Remove BOM and write back
        $newBytes = New-Object byte[] ($bytes.Length - 3)
        [System.Array]::Copy($bytes, 3, $newBytes, 0, $bytes.Length - 3)
        [System.IO.File]::WriteAllBytes($file.FullName, $newBytes)
        $fixed++
        Write-Host "Fixed BOM: $($file.Name)"
    }
}

Write-Host "Fixed $fixed files with BOM"
