# Quick migration: org.jnosql.embed -> org.junify.db
$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path | Split-Path -Parent
$JavaFiles = Get-ChildItem -Recurse -Filter "*.java" (Join-Path $ProjectRoot "src"),(Join-Path $ProjectRoot "spring-boot-starter\src"),(Join-Path $ProjectRoot "quarkus-extension\src"),(Join-Path $ProjectRoot "cli\src") -ErrorAction SilentlyContinue

$mappings = @{
    "org.jnosql.embed" = "org.junify.db"
    "org.jnosql.embed.config" = "org.junify.db.config"
    "org.jnosql.embed.storage" = "org.junify.db.storage.spi"
    "org.jnosql.embed.document" = "org.junify.db.nosql.document"
    "org.jnosql.embed.kv" = "org.junify.db.nosql.kv"
    "org.jnosql.embed.column" = "org.junify.db.nosql.column"
    "org.jnosql.embed.transaction" = "org.junify.db.transaction.mvcc"
    "org.jnosql.embed.eclipse" = "org.junify.db.adapter.jnosql"
    "org.jnosql.embed.index" = "org.junify.db.index"
    "org.jnosql.embed.vector" = "org.junify.db.index.hnsw"
    "org.jnosql.embed.cache" = "org.junify.db.core.cache"
    "org.jnosql.embed.crypto" = "org.junify.db.core.crypto"
    "org.jnosql.embed.backup" = "org.junify.db.core.backup"
    "org.jnosql.embed.metrics" = "org.junify.db.core.metrics"
    "org.jnosql.embed.event" = "org.junify.db.core.event"
    "org.jnosql.embed.health" = "org.junify.db.core.health"
    "org.jnosql.embed.pool" = "org.junify.db.core.pool"
    "org.jnosql.embed.query" = "org.junify.db.sql.query"
    "org.jnosql.embed.reactive" = "org.junify.db.api.reactive"
    "org.jnosql.embed.schema" = "org.junify.db.core.schema"
    "org.jnosql.embed.migration" = "org.junify.db.core.migration"
    "org.jnosql.embed.server" = "org.junify.db.console.http"
    "org.jnosql.embed.util" = "org.junify.db.core.util"
    "org.jnosql.embed.spring" = "org.junify.db.spring"
    "org.jnosql.embed.quarkus" = "org.junify.db.quarkus"
    "org.jnosql.embed.cli" = "org.junify.db.integration.standalone"
}

$count = 0
foreach ($file in $JavaFiles) {
    $content = Get-Content $file.FullName -Raw
    $original = $content
    foreach ($old in $mappings.Keys) {
        $new = $mappings[$old]
        $content = $content -replace ("import\s+" + [regex]::Escape($old) + "\."), ("import " + $new + ".")
        $content = $content -replace ("package\s+" + [regex]::Escape($old) + ";"), ("package " + $new + ";")
        $content = $content -replace ([regex]::Escape($old) + "\."), ($new + ".")
    }
    $content = $content -replace '\bJNoSQL(?=[A-Z_;\s(){}[\]])', 'JunifyDB'
    $content = $content -replace '\bjnosql(?=[\-_])', 'junify'
    $content = $content -replace '\bJNOSQL', 'JUNIFYDB'
    $content = $content -replace '"JNoSQL-EMBED"', '"JunifyDB"'
    if ($content -ne $original) {
        $count++
        Set-Content -Path $file.FullName -Value $content -Encoding UTF8 -NoNewline
    }
}
Write-Host "Updated $count files"
