#!/usr/bin/env pwsh
# Maven wrapper script for PowerShell

$MAVEN_PROJECTBASEDIR = $PSScriptRoot
$MAVEN_WRAPPER_JAR = Join-Path $MAVEN_PROJECTBASEDIR ".mvn\wrapper\maven-wrapper.jar"
$WRAPPER_LAUNCHER = "org.apache.maven.wrapper.MavenWrapperMain"

# Find Java
if ($env:JAVA_HOME) {
    $JAVA_EXE = Join-Path $env:JAVA_HOME "bin\java.exe"
} else {
    $JAVA_EXE = "java"
}

# Check if wrapper JAR exists
if (-not (Test-Path $MAVEN_WRAPPER_JAR)) {
    Write-Host "Downloading Maven wrapper..."
    $wrapperDir = Join-Path $MAVEN_PROJECTBASEDIR ".mvn\wrapper"
    if (-not (Test-Path $wrapperDir)) {
        New-Item -ItemType Directory -Path $wrapperDir | Out-Null
    }
    Invoke-WebRequest -Uri "https://repo.maven.apache.org/maven2/org/apache/maven/wrapper/maven-wrapper/3.2.0/maven-wrapper-3.2.0.jar" -OutFile $MAVEN_WRAPPER_JAR
}

# Run Maven
& $JAVA_EXE -cp $MAVEN_WRAPPER_JAR "-Dmaven.multiModuleProjectDirectory=$MAVEN_PROJECTBASEDIR" $WRAPPER_LAUNCHER $args
