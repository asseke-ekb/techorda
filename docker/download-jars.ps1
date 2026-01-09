# Download required JARs for Iceberg + S3
$jarDir = ".\jars"
New-Item -ItemType Directory -Force -Path $jarDir | Out-Null

$jars = @(
    # Iceberg
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar",
    # AWS SDK for S3
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.4.3/iceberg-aws-bundle-1.4.3.jar",
    # Hadoop AWS
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
    # AWS SDK
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar",
    # PostgreSQL JDBC Driver
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar"
)

foreach ($jar in $jars) {
    $fileName = Split-Path $jar -Leaf
    $outPath = Join-Path $jarDir $fileName

    if (Test-Path $outPath) {
        Write-Host "Already exists: $fileName" -ForegroundColor Yellow
    } else {
        Write-Host "Downloading: $fileName" -ForegroundColor Green
        Invoke-WebRequest -Uri $jar -OutFile $outPath
    }
}

Write-Host "`nAll JARs downloaded to $jarDir" -ForegroundColor Cyan
