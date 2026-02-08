# Exemplo de Uso

## Iniciando o Aplicativo

### Desenvolvimento Local

```bash
# Instalar dependências
go mod download

# Executar
go run .
```

### Docker

```bash
# Build da imagem
docker-compose build

# Executar de forma interativa (permite digitar no terminal)
docker-compose run --rm bybit-notifier

# OU usar o script (Linux)
chmod +x docker-run.sh
./docker-run.sh

# OU usar o script (Windows PowerShell)
.\docker-run.ps1
```

**Importante:** Use `docker-compose run --rm bybit-notifier` para ter interação completa com o terminal. O comando `docker-compose up` não permite digitação interativa.

## Fluxo de Uso

### 1. Cadastrar uma Conta Bybit

Quando você escolher a opção **1** no menu:

```
Nome da conta: Minha Conta Principal
API Key: xxxxxxxxxxxxxx
API Secret: xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
Webhook Discord (opcional): https://discord.com/api/webhooks/xxxxx/xxxxx
```

**Nota:** 
- Se você não fornecer um webhook Discord, as notificações serão exibidas no terminal
- O API Secret nunca será exibido novamente após o cadastro

### 2. Listar Contas

A opção **2** mostrará todas as contas cadastradas:
- Nome da conta
- API Key (mascarado: mostra apenas primeiros e últimos 4 caracteres)
- Status do webhook Discord
- Status da conta (Ativa/Inativa)

### 3. Iniciar Monitoramento

**Opção 5:** Inicia o WebSocket para uma conta específica ou todas as contas ativas

O aplicativo irá:
- Conectar ao WebSocket da Bybit
- Autenticar usando suas credenciais
- Inscrever nos streams `order`, `execution`, `position` e `wallet`
- Monitorar apenas ordens do tipo `inverse`
- Notificar quando ordens forem abertas, canceladas ou stops forem criados/cancelados
- Enviar resumo de posições após 5 minutos sem execuções

### 4. Notificações

**Com Webhook Discord:**
- Mensagem formatada com ícone de alerta (🔔), detalhes da operação e timestamp no horário de Brasília

**Sem Webhook Discord:**
- As notificações não são exibidas no terminal quando não há webhook configurado

### 5. Reconexão Automática

O aplicativo automaticamente:
- Reconecta se a conexão WebSocket cair
- Usa exponential backoff (5s, 10s, 20s, 40s... até 5 minutos)
- Mantém tentativas infinitas de reconexão
- Restaura conexões ativas ao reiniciar o aplicativo

## Exemplos de Notificações

### Nova Ordem Aberta (Única)

```
🔔
🟢 Nova ordem aberta - BTCUSD Sell Market @ 45000.00 (Qty: 100.00 USD)

🕘  15/12/2024 - 14:30 (Horário de Brasília)
```

### Múltiplas Ordens Agrupadas (Mesmo Preço)

```
🔔
🟢 3 ordens Sell Market agrupadas - BTCUSD @ 45000.00 (Qty Total: 300.00 USD)

🕘  15/12/2024 - 14:30 (Horário de Brasília)
```

### Múltiplas Ordens Agrupadas (Range de Preços)

```
🔔
🟢 5 ordens Buy Limit agrupadas - ETHUSD
   Range: 2500.00 até 2550.00
   Qty Total: 500.00 USD

🕘  15/12/2024 - 14:30 (Horário de Brasília)
```

### Ordem com Reduce Only

```
🔔
🟢 Nova ordem aberta - BTCUSD Reduce Sell Market @ 45000.00 (Qty: 50.00 USD)

🕘  15/12/2024 - 14:30 (Horário de Brasília)
```

### Ordens Canceladas

```
🔔
❌ 2 ordens canceladas:
  • BTCUSD Buy Limit @ 44000.00
  • ETHUSD Sell Limit @ 2600.00

🕘  15/12/2024 - 14:30 (Horário de Brasília)
```

### Stop Order Criado (Buy)

```
🔔
🟢 Stop Buy Market - BTCUSD @ 46000.00 (Qty: 100.00 USD)

🕘  15/12/2024 - 14:30 (Horário de Brasília)
```

### Stop Order Criado (Sell)

```
🔔
🔴 Stop Sell Market - BTCUSD @ 44000.00 (Qty: 100.00 USD)

🕘  15/12/2024 - 14:30 (Horário de Brasília)
```

### Stop Order Cancelado

```
🔔
❌ 🟢 Stop Buy Market **CANCELADO** - BTCUSD @ 46000.00 (Qty: 100.00 USD)

🕘  15/12/2024 - 14:30 (Horário de Brasília)
```

### Resumo de Posições (Após 5 Minutos Sem Execuções)

**Com uma única posição válida:**
```
🔔
📌 BTC (BTCUSD):
  💰 Total: $10000.00 USD
  🛡️ Protegido: $5000.00 USD
  ⚠️ Exposto: $5000.00 USD
  📈 % Protegida: 50.00%

🕘  15/12/2024 - 14:35 (Horário de Brasília)
```

**Com múltiplas posições ou nenhuma posição válida:**
```
🔔
📌 BTC (BTCUSD):
  💰 Total: $10000.00 USD
  🛡️ Protegido: $5000.00 USD
  ⚠️ Exposto: $5000.00 USD
  📈 % Protegida: 50.00%

📌 ETH (ETHUSD):
  💰 Total: $5000.00 USD
  🛡️ Protegido: $2000.00 USD
  📈 Posição Long: $1000.00 USD
  ⚠️ Exposto: $2000.00 USD
  📈 % Protegida: 40.00%
  📊 % Longada: 20.00%

📊 Resumo Geral:
  💰 Carteira Total: $15000.00 USD
  🛡️ Proteção Total: $7000.00 USD
  📈 Long Total: $1000.00 USD
  ⚠️ Exposição Total: $7000.00 USD
  📈 % Protegida: 46.67%
  📊 % Longada: 6.67%

🕘  15/12/2024 - 14:35 (Horário de Brasília)
```

## Persistência

O banco de dados SQLite (`bybit_accounts.db`) armazena:
- Todas as contas cadastradas
- Estado das conexões ativas

Ao reiniciar o aplicativo, todas as conexões que estavam ativas serão automaticamente restauradas.

