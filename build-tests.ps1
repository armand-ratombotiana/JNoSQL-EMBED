$env:JAVA_HOME = "C:\Users\judic\scoop\apps\openjdk24\24.0.2-12"
$env:PATH = "C:\Users\judic\scoop\apps\maven\3.9.14\bin;$env:PATH"
$env:MAVEN_OPTS = "-Xmx512m"

Write-Host "=== Compiling Main Project ===" -ForegroundColor Cyan
mvn clean compile -DskipTests -q

if ($LASTEXITCODE -eq 0) {
    Write-Host "Main project compiled successfully!" -ForegroundColor Green
} else {
    Write-Host "Main project compilation failed!" -ForegroundColor Red
    exit $LASTEXITCODE
}

Write-Host "`n=== Compiling Test Projects ===" -ForegroundColor Cyan

# Compile simple-java test
Set-Location test-project\simple-java
Write-Host "Compiling simple-java test..." -ForegroundColor Yellow
mvn clean compile -DskipTests -q 2>$null
if ($LASTEXITCODE -eq 0) {
    Write-Host "simple-java: SUCCESS" -ForegroundColor Green
} else {
    Write-Host "simple-java: FAILED (exit code: $LASTEXITCODE)" -ForegroundColor Red
}

# Go back to root
Set-Location ..\..

Write-Host "`n=== All Compilations Complete ===" -ForegroundColor Cyan