$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot

Write-Host "Starting Kafka, producer, consumer, and DLQ monitor..."
docker compose up --build
