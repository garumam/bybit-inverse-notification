# Script de build para Windows

Write-Host "Criando diretório bin..."
New-Item -ItemType Directory -Force -Path bin | Out-Null

Write-Host "Building for Windows..."
$env:CGO_ENABLED = "1"
go build -o bin/bybit-notifier-windows.exe -ldflags="-s -w" .

Write-Host "Build concluído! Arquivo em ./bin/"

