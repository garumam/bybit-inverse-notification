FROM golang:1.21 AS builder

# Instalar dependências necessárias para CGO e SQLite (Debian-based)
RUN apt-get update && apt-get install -y \
    gcc \
    libc6-dev \
    libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copiar todos os arquivos (go.mod e código fonte)
COPY . .

# Gerar go.sum e baixar dependências (precisa dos arquivos .go para gerar go.sum corretamente)
RUN go mod tidy && go mod download

# Build da aplicação
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" \
    -o bybit-notifier .

FROM debian:bookworm-slim

# Instalar SQLite runtime e ca-certificates
RUN apt-get update && apt-get install -y \
    ca-certificates \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copiar binário do builder
COPY --from=builder /app/bybit-notifier .

# Volume para persistir banco de dados
VOLUME ["/app/data"]

# Manter container rodando
CMD ["./bybit-notifier"]

