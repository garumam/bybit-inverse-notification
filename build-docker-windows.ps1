# Script para gerar build do Windows usando Docker (Windows PowerShell)
# Não requer Go instalado localmente

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Build Windows via Docker" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Verificar se Docker está instalado
try {
    docker --version | Out-Null
} catch {
    Write-Host "ERRO: Docker não está instalado ou não está no PATH!" -ForegroundColor Red
    Write-Host "Por favor, instale o Docker Desktop: https://www.docker.com/products/docker-desktop" -ForegroundColor Yellow
    exit 1
}

Write-Host "Criando diretório bin..." -ForegroundColor Cyan
New-Item -ItemType Directory -Force -Path bin | Out-Null

Write-Host "Construindo imagem Docker para build do Windows..." -ForegroundColor Cyan
docker-compose -f docker-compose.build.windows.yml build

if ($LASTEXITCODE -ne 0) {
    Write-Host "ERRO: Falha ao construir a imagem Docker!" -ForegroundColor Red
    exit 1
}

Write-Host "Gerando build para Windows..." -ForegroundColor Cyan
Write-Host "(Isso pode levar alguns minutos...)" -ForegroundColor Yellow
Write-Host ""
docker-compose -f docker-compose.build.windows.yml run --rm builder

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Green
    Write-Host "  Build concluído com sucesso!" -ForegroundColor Green
    Write-Host "========================================" -ForegroundColor Green
    Write-Host ""
    Write-Host "Arquivo gerado em ./bin/:" -ForegroundColor Cyan
    Get-ChildItem -Path bin -Filter "bybit-notifier-windows.exe" | Format-Table Name, @{Label="Tamanho"; Expression={"{0:N2} KB" -f ($_.Length / 1KB)}}, LastWriteTime
} else {
    Write-Host ""
    Write-Host "ERRO: Falha ao gerar o build!" -ForegroundColor Red
    exit 1
}

