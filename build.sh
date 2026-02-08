#!/bin/bash

# Script de build para Linux

echo "Criando diretório bin..."
mkdir -p bin

echo "Building for Linux..."
CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o bin/bybit-notifier-linux -ldflags="-s -w" .

echo "Builds concluídos! Arquivos em ./bin/"

