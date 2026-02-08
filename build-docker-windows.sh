#!/bin/bash

# Script para gerar build do Windows usando Docker (Linux)
# Não requer Go instalado localmente

set -e

echo "========================================"
echo "  Build Windows via Docker"
echo "========================================"
echo ""

# Verificar se Docker está instalado
if ! command -v docker &> /dev/null; then
    echo "ERRO: Docker não está instalado!"
    echo "Por favor, instale o Docker: https://docs.docker.com/get-docker/"
    exit 1
fi

echo "Criando diretório bin..."
mkdir -p bin

echo "Construindo imagem Docker para build do Windows..."
docker-compose -f docker-compose.build.windows.yml build

echo "Gerando build para Windows..."
echo "(Isso pode levar alguns minutos...)"
echo ""
docker-compose -f docker-compose.build.windows.yml run --rm builder

if [ $? -eq 0 ]; then
    echo ""
    echo "========================================"
    echo "  Build concluído com sucesso!"
    echo "========================================"
    echo ""
    echo "Arquivo gerado em ./bin/:"
    ls -lh bin/bybit-notifier-windows.exe
else
    echo ""
    echo "ERRO: Falha ao gerar o build!"
    exit 1
fi

