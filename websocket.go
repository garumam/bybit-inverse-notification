package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

const (
	bybitWSURL = "wss://stream.bybit.com/v5/private"
)

type WebSocketManager struct {
	accountManager *AccountManager
	db             *Database
	connections    map[int64]*WebSocketConnection
	mu             sync.RWMutex
	// Buffers para agrupamento de mensagens
	orderBuffers    map[int64]*OrderBuffer
	cancelBuffers   map[int64]*CancelBuffer
	executionBuffers map[int64]*ExecutionBuffer
	bufferMu        sync.RWMutex
}

type OrderBuffer struct {
	orders    []OrderData
	timer     *time.Timer
	mu        sync.Mutex
	accountID int64
}

type CancelBuffer struct {
	orders    []OrderData
	timer     *time.Timer
	mu        sync.Mutex
	accountID int64
}

type ExecutionBuffer struct {
	lastWallet   *WalletData
	positions    map[string]*PositionData // Mapa por símbolo (ex: BTCUSD)
	timer        *time.Timer
	mu           sync.Mutex
	accountID    int64
}

type WebSocketConnection struct {
	AccountID  int64
	Account    *BybitAccount
	Conn       *websocket.Conn
	StopChan   chan struct{}
	Running    bool
	mu         sync.Mutex
}

type BybitOrderMessage struct {
	ID           string      `json:"id"`
	Topic        string      `json:"topic"`
	CreationTime int64       `json:"creationTime"`
	Data         []OrderData `json:"data"`
}

type BybitExecutionMessage struct {
	ID           string          `json:"id"`
	Topic        string          `json:"topic"`
	CreationTime int64           `json:"creationTime"`
	Data         []ExecutionData `json:"data"`
}

type BybitPositionMessage struct {
	ID           string         `json:"id"`
	Topic        string         `json:"topic"`
	CreationTime int64          `json:"creationTime"`
	Data         []PositionData `json:"data"`
}

type BybitWalletMessage struct {
	ID           string       `json:"id"`
	Topic        string       `json:"topic"`
	CreationTime int64        `json:"creationTime"`
	Data         []WalletData `json:"data"`
}

type WalletData struct {
	AccountType            string        `json:"accountType"`
	AccountIMRate          string        `json:"accountIMRate"`
	AccountMMRate          string        `json:"accountMMRate"`
	TotalEquity            string        `json:"totalEquity"`
	TotalWalletBalance     string        `json:"totalWalletBalance"`
	TotalMarginBalance     string        `json:"totalMarginBalance"`
	TotalAvailableBalance  string        `json:"totalAvailableBalance"`
	TotalPerpUPL           string        `json:"totalPerpUPL"`
	TotalInitialMargin     string        `json:"totalInitialMargin"`
	TotalMaintenanceMargin string        `json:"totalMaintenanceMargin"`
	Coin                   []CoinBalance `json:"coin"`
}

type ExecutionData struct {
	Category      string `json:"category"`
	Symbol        string `json:"symbol"`
	ExecType      string `json:"execType"`
	ExecPrice     string `json:"execPrice"`
	ExecQty       string `json:"execQty"`
	ExecValue     string `json:"execValue"`
	Side          string `json:"side"`
	OrderID       string `json:"orderId"`
	OrderLinkID   string `json:"orderLinkId"`
	OrderType     string `json:"orderType"`
	CreateType    string `json:"createType"`
	MarkPrice     string `json:"markPrice"`
}

type PositionData struct {
	Symbol          string `json:"symbol"`
	Side            string `json:"side"`
	Size            string `json:"size"`
	EntryPrice      string `json:"entryPrice"`
	MarkPrice       string `json:"markPrice"`
	PositionValue   string `json:"positionValue"`
	PositionIM      string `json:"positionIM"`
	PositionMM      string `json:"positionMM"`
	UnrealisedPnl   string `json:"unrealisedPnl"`
	StopLoss        string `json:"stopLoss"`
	TakeProfit      string `json:"takeProfit"`
	Category        string `json:"category"`
	PositionStatus  string `json:"positionStatus"`
}

type CoinBalance struct {
	Coin            string `json:"coin"`
	Equity          string `json:"equity"`
	UsdValue        string `json:"usdValue"`
	WalletBalance   string `json:"walletBalance"`
	AvailableToWithdraw string `json:"availableToWithdraw"`
	UnrealisedPnl   string `json:"unrealisedPnl"`
	CumRealisedPnl  string `json:"cumRealisedPnl"`
}

type OrderData struct {
	Category      string `json:"category"`
	OrderID       string `json:"orderId"`
	OrderLinkID   string `json:"orderLinkId"`
	Symbol        string `json:"symbol"`
	Side          string `json:"side"`
	OrderType     string `json:"orderType"`
	OrderStatus   string `json:"orderStatus"`
	CancelType    string `json:"cancelType"`
	RejectReason  string `json:"rejectReason"`
	Price         string `json:"price"`
	AvgPrice      string `json:"avgPrice"`
	Qty           string `json:"qty"`
	CreatedTime   string `json:"createdTime"`
	UpdatedTime   string `json:"updatedTime"`
	ReduceOnly    bool   `json:"reduceOnly"`
	StopOrderType string `json:"stopOrderType"`
	TriggerPrice  string `json:"triggerPrice"`
	CreateType    string `json:"createType"`
}

func NewWebSocketManager(db *Database, accountManager *AccountManager) *WebSocketManager {
	return &WebSocketManager{
		accountManager:   accountManager,
		db:               db,
		connections:      make(map[int64]*WebSocketConnection),
		orderBuffers:     make(map[int64]*OrderBuffer),
		cancelBuffers:    make(map[int64]*CancelBuffer),
		executionBuffers: make(map[int64]*ExecutionBuffer),
	}
}

func (wsm *WebSocketManager) StartConnection(accountID int64) error {
	wsm.mu.Lock()
	defer wsm.mu.Unlock()

	if _, exists := wsm.connections[accountID]; exists {
		return fmt.Errorf("conexão já está ativa para esta conta")
	}

	account, err := wsm.accountManager.GetAccount(accountID)
	if err != nil {
		return err
	}

	wsConn := &WebSocketConnection{
		AccountID: accountID,
		Account:   account,
		StopChan:  make(chan struct{}),
		Running:   true,
	}

	wsm.connections[accountID] = wsConn

	// Marcar como ativa no banco
	if err := wsm.accountManager.SetConnectionActive(accountID, true); err != nil {
		// Erro silencioso - tentar novamente na próxima vez
	}

	// Iniciar conexão em goroutine com tratamento de panic
	go func() {
		defer func() {
			if r := recover(); r != nil {
				// Imprimir no stderr PRIMEIRO (antes de tentar qualquer coisa)
				fmt.Fprintf(os.Stderr, "\n=== ERRO FATAL ===\n")
				fmt.Fprintf(os.Stderr, "A aplicação encontrou um erro fatal ao iniciar o monitoramento da conta '%s' (ID: %d)\n", account.Name, accountID)
				fmt.Fprintf(os.Stderr, "Erro: %v\n", r)
				
				// Tentar logar o panic (mas não bloquear se falhar)
				func() {
					defer func() {
						if r2 := recover(); r2 != nil {
							fmt.Fprintf(os.Stderr, "ERRO ao tentar logar o panic: %v\n", r2)
						}
					}()
					logger, _ := getLogger(accountID, account.Name)
					if logger != nil {
						logger.Log("PANIC fatal em runConnection (goroutine): %v", r)
					}
				}()
				
				// Tentar obter caminho do log (usar padrão comum)
				var logPath string
				if dataDir := os.Getenv("DATA_DIR"); dataDir != "" {
					logPath = filepath.Join(dataDir, "logs", fmt.Sprintf("account_%d.log", accountID))
				} else {
					logPath = filepath.Join("data", "logs", fmt.Sprintf("account_%d.log", accountID))
				}
				fmt.Fprintf(os.Stderr, "Verifique os logs em: %s\n", logPath)
				fmt.Fprintf(os.Stderr, "==================\n\n")
			}
		}()
		
		wsm.runConnection(wsConn)
	}()

	return nil
}

func (wsm *WebSocketManager) StopConnection(accountID int64) {
	wsm.mu.Lock()
	defer wsm.mu.Unlock()

	conn, exists := wsm.connections[accountID]
	if !exists {
		return
	}

	conn.mu.Lock()
	if conn.Running {
		close(conn.StopChan)
		conn.Running = false
		if conn.Conn != nil {
			conn.Conn.Close()
		}
	}
	conn.mu.Unlock()

	delete(wsm.connections, accountID)

	// Limpar buffers
	wsm.bufferMu.Lock()
	if orderBuffer, exists := wsm.orderBuffers[accountID]; exists {
		if orderBuffer.timer != nil {
			orderBuffer.timer.Stop()
		}
		delete(wsm.orderBuffers, accountID)
	}
	if cancelBuffer, exists := wsm.cancelBuffers[accountID]; exists {
		if cancelBuffer.timer != nil {
			cancelBuffer.timer.Stop()
		}
		delete(wsm.cancelBuffers, accountID)
	}
	if executionBuffer, exists := wsm.executionBuffers[accountID]; exists {
		if executionBuffer.timer != nil {
			executionBuffer.timer.Stop()
		}
		delete(wsm.executionBuffers, accountID)
	}
	wsm.bufferMu.Unlock()

	// Fechar logger
	closeLogger(accountID)

	// Remover do banco
	wsm.accountManager.SetConnectionActive(accountID, false)
}

func (wsm *WebSocketManager) StopAll() {
	wsm.mu.Lock()
	defer wsm.mu.Unlock()

	for accountID, conn := range wsm.connections {
		conn.mu.Lock()
		if conn.Running {
			close(conn.StopChan)
			conn.Running = false
			if conn.Conn != nil {
				conn.Conn.Close()
			}
		}
		conn.mu.Unlock()

		wsm.accountManager.SetConnectionActive(accountID, false)
	}

	wsm.connections = make(map[int64]*WebSocketConnection)
}

func (wsm *WebSocketManager) IsConnectionActive(accountID int64) bool {
	wsm.mu.RLock()
	defer wsm.mu.RUnlock()

	conn, exists := wsm.connections[accountID]
	return exists && conn.Running
}

func (wsm *WebSocketManager) StartAllConnections() error {
	accounts, err := wsm.accountManager.ListAccounts()
	if err != nil {
		return err
	}

	for _, account := range accounts {
		if account.Active {
			if err := wsm.StartConnection(account.ID); err != nil {
				// Erro já será logado pelo logger na função StartConnection
			}
		}
	}

	return nil
}

func (wsm *WebSocketManager) RestoreConnections() error {
	accountIDs, err := wsm.accountManager.GetActiveConnections()
	if err != nil {
		return err
	}

	for _, accountID := range accountIDs {
		if err := wsm.StartConnection(accountID); err != nil {
			// Erro já será logado pelo logger na função StartConnection
		} else {
			// Conexão restaurada - já será logado pelo logger
		}
	}

	return nil
}

func (wsm *WebSocketManager) runConnection(wsConn *WebSocketConnection) {
	// Capturar panics para evitar crash silencioso
	defer func() {
		if r := recover(); r != nil {
			// Imprimir no stderr PRIMEIRO
			fmt.Fprintf(os.Stderr, "[PANIC] runConnection para conta %d: %v\n", wsConn.AccountID, r)
			
			// Tentar logar o panic (mas não bloquear se falhar)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar o panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em runConnection: %v", r)
				}
			}()
			
			// Re-throw para que seja visível
			panic(r)
		}
	}()

	maxRetries := 999999 // Reconexão infinita
	retryDelay := time.Second * 5
	maxRetryDelay := time.Minute * 5
	initialRetryDelay := retryDelay

	logger, err := getLogger(wsConn.AccountID, wsConn.Account.Name)
	if err != nil {
		// Se não conseguir criar logger, pelo menos imprimir no stderr
		fmt.Fprintf(os.Stderr, "ERRO: Não foi possível criar logger para conta %d: %v\n", wsConn.AccountID, err)
	}

	consecutiveFailures := 0
	maxConsecutiveFailures := 10 // Após 10 falhas consecutivas, fazer limpeza forçada

	for retry := 0; retry < maxRetries; retry++ {
		select {
		case <-wsConn.StopChan:
			return
		default:
		}

		// Limpar conexão antiga antes de tentar nova conexão
		wsConn.mu.Lock()
		if wsConn.Conn != nil {
			// Fechar conexão antiga silenciosamente
			wsConn.Conn.Close()
			wsConn.Conn = nil
		}
		wsConn.mu.Unlock()

		// Se houver muitas falhas consecutivas, fazer uma limpeza mais agressiva
		if consecutiveFailures >= maxConsecutiveFailures {
			if logger != nil {
				logger.Log("⚠️ Muitas falhas consecutivas (%d), fazendo limpeza forçada e aguardando antes de reconectar...", consecutiveFailures)
			}
			// Resetar delay e aguardar mais tempo
			retryDelay = initialRetryDelay
			consecutiveFailures = 0
			select {
			case <-wsConn.StopChan:
				return
			case <-time.After(30 * time.Second):
			}
		}

		if err := wsm.connectAndListen(wsConn); err != nil {
			// Verificar se foi parado manualmente
			select {
			case <-wsConn.StopChan:
				return
			default:
			}

			consecutiveFailures++
			if logger != nil {
				logger.Log("Erro na conexão WebSocket (tentativa %d, falhas consecutivas: %d): %v", retry+1, consecutiveFailures, err)
			}

			// Exponential backoff com limite máximo
			select {
			case <-wsConn.StopChan:
				return
			case <-time.After(retryDelay):
				if retryDelay < maxRetryDelay {
					retryDelay *= 2
				}
			}
		} else {
			// Conexão bem-sucedida - resetar contadores e delays
			consecutiveFailures = 0
			retryDelay = initialRetryDelay
			
			// Conexão fechada normalmente, verificar se deve reconectar
			select {
			case <-wsConn.StopChan:
				return
			default:
				// Reconectar após um delay curto
				if logger != nil {
					logger.Log("Conexão fechada, tentando reconectar...")
				}
				time.Sleep(initialRetryDelay)
			}
		}
	}
}

func (wsm *WebSocketManager) connectAndListen(wsConn *WebSocketConnection) (err error) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] connectAndListen para conta %d: %v\n", wsConn.AccountID, r)
			
			// Tentar logar o panic (mas não bloquear se falhar)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar o panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em connectAndListen: %v", r)
				}
			}()
			
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	logger, logErr := getLogger(wsConn.AccountID, wsConn.Account.Name)
	if logErr != nil {
		fmt.Fprintf(os.Stderr, "ERRO: Não foi possível criar logger para conta %d: %v\n", wsConn.AccountID, logErr)
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(bybitWSURL, nil)
	if err != nil {
		if logger != nil {
			logger.Log("Erro ao conectar: %v", err)
		}
		return fmt.Errorf("erro ao conectar: %w", err)
	}

	wsConn.mu.Lock()
	// Fechar conexão antiga se existir
	if wsConn.Conn != nil {
		wsConn.Conn.Close()
	}
	wsConn.Conn = conn
	wsConn.mu.Unlock()

	// Garantir que a conexão seja fechada ao sair
	defer func() {
		wsConn.mu.Lock()
		if wsConn.Conn == conn {
			wsConn.Conn.Close()
			wsConn.Conn = nil
		}
		wsConn.mu.Unlock()
		conn.Close()
	}()

	// Autenticar
	if err := wsm.authenticate(conn, wsConn.Account); err != nil {
		if logger != nil {
			logger.Log("Erro na autenticação: %v", err)
		}
		return fmt.Errorf("erro na autenticação: %w", err)
	}

	// Aguardar resposta de autenticação
	time.Sleep(1 * time.Second)

	// Inscrever nos tópicos order, execution, position e wallet
	subscribeMsg := map[string]interface{}{
		"op":   "subscribe",
		"args": []string{"order", "execution", "position", "wallet"},
	}

	if err := conn.WriteJSON(subscribeMsg); err != nil {
		if logger != nil {
			logger.Log("Erro ao inscrever: %v", err)
		}
		return fmt.Errorf("erro ao inscrever: %w", err)
	}

	if logger != nil {
		logger.Log("WebSocket conectado, autenticado e inscrito nos tópicos 'order', 'execution', 'position' e 'wallet'")
	}

	// Configurar timeouts e pong handler
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// Criar um canal de stop específico para o pingLoop desta conexão
	pingStopChan := make(chan struct{})
	
	// Iniciar ping em goroutine separada
	go wsm.pingLoop(conn, pingStopChan)
	
	// Garantir que o pingLoop seja parado quando sair desta função
	defer close(pingStopChan)

	// Ler mensagens
	for {
		select {
		case <-wsConn.StopChan:
			return nil
		default:
			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				// Verificar se foi parado manualmente
				select {
				case <-wsConn.StopChan:
					return nil
				default:
				}

				// Verificar se é um erro de fechamento
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					// Fechamento normal - retornar nil para reconectar
					if logger != nil {
						logger.Log("Conexão fechada normalmente pelo servidor")
					}
					return nil
				}

				// Para erros 1006 (abnormal closure) e outros erros inesperados,
				// garantir que a conexão seja limpa antes de retornar
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					if logger != nil {
						logger.Log("Erro inesperado de fechamento: %v", err)
					}
					// Limpar conexão antes de retornar
					wsConn.mu.Lock()
					if wsConn.Conn == conn {
						wsConn.Conn = nil
					}
					wsConn.mu.Unlock()
					return fmt.Errorf("erro ao ler mensagem: %w", err)
				}
				
				// Timeout ou erro de leitura, reconectar
				if logger != nil {
					logger.Log("Erro na leitura (timeout ou outro): %v", err)
				}
				// Limpar conexão antes de retornar
				wsConn.mu.Lock()
				if wsConn.Conn == conn {
					wsConn.Conn = nil
				}
				wsConn.mu.Unlock()
				return fmt.Errorf("erro na leitura: %w", err)
			}

			// Processar apenas mensagens de texto
			if messageType == websocket.TextMessage {
				wsm.handleMessage(wsConn, message)
			}
		}
	}
}

func (wsm *WebSocketManager) authenticate(conn *websocket.Conn, account *BybitAccount) error {
	// Limpar espaços das credenciais
	apiKey := strings.TrimSpace(account.APIKey)
	apiSecret := strings.TrimSpace(account.APISecret)

	// Gerar timestamp atual em milissegundos (como número, não string)
	// A biblioteca oficial adiciona 10 segundos (10000ms) ao timestamp
	expires := time.Now().UnixNano()/1e6 + 10000

	// Criar string para assinatura: GET/realtime{expires}
	// IMPORTANTE: expires é um número, não string
	signatureString := fmt.Sprintf("GET/realtime%d", expires)

	// Calcular HMAC SHA256
	mac := hmac.New(sha256.New, []byte(apiSecret))
	mac.Write([]byte(signatureString))
	
	// IMPORTANTE: A biblioteca oficial usa HEX, não base64!
	signature := hex.EncodeToString(mac.Sum(nil))


	// Criar mensagem de autenticação conforme biblioteca oficial
	// Formato: {"req_id": "uuid", "op": "auth", "args": [apiKey, expires, signature]}
	// expires e signature são números/hex, não strings
	authMsg := map[string]interface{}{
		"req_id": uuid.New().String(),
		"op":     "auth",
		"args":   []interface{}{apiKey, expires, signature},
	}

	// Serializar manualmente para garantir formato exato (sem espaços extras)
	jsonData, err := json.Marshal(authMsg)
	if err != nil {
		return fmt.Errorf("erro ao serializar mensagem de autenticação: %w", err)
	}

	// Enviar como mensagem de texto (não JSON struct)
	if err := conn.WriteMessage(websocket.TextMessage, jsonData); err != nil {
		return fmt.Errorf("erro ao enviar mensagem de autenticação: %w", err)
	}

	// Aguardar resposta de autenticação
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var authResponse map[string]interface{}
	if err := conn.ReadJSON(&authResponse); err != nil {
		return fmt.Errorf("erro ao ler resposta de autenticação: %w", err)
	}

	// Verificar se autenticação foi bem-sucedida
	if success, ok := authResponse["success"].(bool); ok && !success {
		retMsg, _ := authResponse["ret_msg"].(string)
		return fmt.Errorf("autenticação falhou: %s (resposta: %v)", retMsg, authResponse)
	}

	// Verificar se success é true
	if success, ok := authResponse["success"].(bool); ok && success {
		logger, _ := getLogger(account.ID, account.Name)
		if logger != nil {
			logger.Log("✅ Autenticação bem-sucedida")
		}
		return nil
	}

	return fmt.Errorf("resposta de autenticação inesperada: %v", authResponse)
}

func (wsm *WebSocketManager) pingLoop(conn *websocket.Conn, stopChan chan struct{}) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "PANIC em pingLoop: %v\n", r)
		}
	}()

	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			// Verificar se o canal foi fechado antes de tentar escrever
			select {
			case <-stopChan:
				return
			default:
			}
			
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				// Erro ao enviar ping, a conexão será detectada no loop principal
				// Não fazer nada, apenas retornar para parar o loop
				return
			}
		}
	}
}

func (wsm *WebSocketManager) handleMessage(wsConn *WebSocketConnection, message []byte) {
	// Capturar panics para evitar crash silencioso
	defer func() {
		if r := recover(); r != nil {
			logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
			if logger != nil {
				logger.Log("PANIC em handleMessage: %v", r)
			} else {
				fmt.Fprintf(os.Stderr, "PANIC em handleMessage (conta %d): %v\n", wsConn.AccountID, r)
			}
		}
	}()

	logger, logErr := getLogger(wsConn.AccountID, wsConn.Account.Name)
	if logErr != nil {
		fmt.Fprintf(os.Stderr, "ERRO: Não foi possível criar logger em handleMessage para conta %d: %v\n", wsConn.AccountID, logErr)
	}

	// Tentar parsear como mensagem de controle primeiro
	var controlMsg map[string]interface{}
	if err := json.Unmarshal(message, &controlMsg); err == nil {
		if op, ok := controlMsg["op"].(string); ok {
			if op == "auth" {
				if logger != nil {
					logger.Log("[DEBUG] Resposta de autenticação: %v", controlMsg)
				}
				return
			}
			if op == "subscribe" {
				if success, ok := controlMsg["success"].(bool); ok && success {
					if logger != nil {
						logger.Log("✅ Inscrição nos tópicos confirmada!")
					}
				} else {
					if logger != nil {
						logger.Log("⚠️ Inscrição pode ter falhado: %v", controlMsg)
					}
				}
				return
			}
			if op == "ping" || op == "pong" {
				// Pings/pongs são normais, não logar
				return
			}
		}
		// Se tem campo "topic", pode ser uma mensagem de dados
		if topic, ok := controlMsg["topic"].(string); ok {
			if logger != nil {
				logger.Log("[DEBUG] Mensagem com tópico recebida: topic=%s", topic)
			}
		}
	}

	// Tentar parsear como mensagem de order
	var orderMsg BybitOrderMessage
	if err := json.Unmarshal(message, &orderMsg); err == nil && orderMsg.Topic == "order" {
		wsm.handleOrderMessage(wsConn, orderMsg)
		return
	}

	// Tentar parsear como mensagem de execution
	var execMsg BybitExecutionMessage
	if err := json.Unmarshal(message, &execMsg); err == nil && execMsg.Topic == "execution" {
		wsm.handleExecutionMessage(wsConn, execMsg)
		return
	}

	// Tentar parsear como mensagem de position
	var posMsg BybitPositionMessage
	if err := json.Unmarshal(message, &posMsg); err == nil && posMsg.Topic == "position" {
		wsm.handlePositionMessage(wsConn, posMsg)
		return
	}

	// Tentar parsear como mensagem de wallet
	var walletMsg BybitWalletMessage
	if err := json.Unmarshal(message, &walletMsg); err == nil && walletMsg.Topic == "wallet" {
		wsm.handleWalletMessage(wsConn, walletMsg)
		return
	}
}

func (wsm *WebSocketManager) handleOrderMessage(wsConn *WebSocketConnection, orderMsg BybitOrderMessage) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] handleOrderMessage para conta %d: %v\n", wsConn.AccountID, r)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em handleOrderMessage: %v", r)
				}
			}()
		}
	}()

	logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)

	if logger != nil {
		logger.Log("[DEBUG] Mensagem de order recebida! Total de ordens: %d", len(orderMsg.Data))
	}

	for _, orderData := range orderMsg.Data {
		if logger != nil {
			logger.Log("[DEBUG] Processando ordem - Category: %s, Status: %s, Symbol: %s",
				orderData.Category, orderData.OrderStatus, orderData.Symbol)
		}

		// Processar apenas ordens inverse
		if orderData.Category != "inverse" {
			if logger != nil {
				logger.Log("[DEBUG] Ordem ignorada - não é inverse (category: %s)", orderData.Category)
			}
			continue
		}

		// Ignorar ordens com rejectReason diferente de EC_NoError
		// Isso evita processar mensagens duplicadas quando uma ordem é executada
		// e ao mesmo tempo há uma tentativa de cancelamento
		if orderData.RejectReason != "" && orderData.RejectReason != "EC_NoError" && orderData.RejectReason != "EC_PerCancelRequest" {
			if logger != nil {
				logger.Log("[DEBUG] Ordem ignorada - rejectReason diferente de EC_NoError: %s", orderData.RejectReason)
			}
			continue
		}

		// Processar stops Untriggered primeiro (sem delay)
		if orderData.OrderStatus == "Untriggered" {
			wsm.processStopOrder(wsConn, orderData)
			continue
		}

		// Processar cancelamentos de stops não triggerados (Deactivated)
		// Permitir processar mesmo se for CreateByStopOrder
		if orderData.OrderStatus == "Deactivated" {
			wsm.processStopCancellation(wsConn, orderData)
			continue
		}

		// Ignorar ordens criadas por stop order (exceto stops Untriggered e Deactivated já processados acima)
		if orderData.CreateType == "CreateByStopOrder" {
			if logger != nil {
				logger.Log("[DEBUG] Ordem ignorada - CreateByStopOrder (status: %s)", orderData.OrderStatus)
			}
			continue
		}

		// Processar abertura de ordem ou cancelamento
		// Verificar se é Limit executada rapidamente (até 3 segundos entre criação e atualização)
		isLimitExecutedQuickly := false
		if orderData.OrderType == "Limit" && (orderData.OrderStatus == "Filled" || orderData.OrderStatus == "PartiallyFilled") {
			createdTime, err1 := strconv.ParseInt(orderData.CreatedTime, 10, 64)
			updatedTime, err2 := strconv.ParseInt(orderData.UpdatedTime, 10, 64)
			if err1 == nil && err2 == nil {
				timeDiff := updatedTime - createdTime
				if timeDiff >= 0 && timeDiff <= 3000 { // Diferença de até 3 segundos (3000ms)
					isLimitExecutedQuickly = true
				}
			}
		}
		
		if orderData.OrderStatus == "New" || (orderData.OrderType == "Market" && (orderData.OrderStatus == "Filled" || orderData.OrderStatus == "PartiallyFilled")) || isLimitExecutedQuickly {
			wsm.addOrderToBuffer(wsConn.AccountID, orderData, wsConn)
		} else if orderData.OrderStatus == "Cancelled" || (orderData.CancelType != "" && orderData.StopOrderType != "Stop" && orderData.OrderStatus != "Filled") {
			// Excluir stops do processamento de cancelamento normal
			wsm.addCancelToBuffer(wsConn.AccountID, orderData, wsConn)
		}
	}
}

func (wsm *WebSocketManager) addOrderToBuffer(accountID int64, order OrderData, wsConn *WebSocketConnection) {
	wsm.bufferMu.Lock()
	buffer, exists := wsm.orderBuffers[accountID]
	if !exists {
		buffer = &OrderBuffer{
			orders:    []OrderData{},
			accountID: accountID,
		}
		wsm.orderBuffers[accountID] = buffer
	}
	wsm.bufferMu.Unlock()

	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	// Adicionar ordem ao buffer
	buffer.orders = append(buffer.orders, order)

	logger, _ := getLogger(accountID, wsConn.Account.Name)

	// Se é a primeira ordem, iniciar timer de 2 segundos
	if len(buffer.orders) == 1 {
		buffer.timer = time.AfterFunc(2*time.Second, func() {
			defer func() {
				if r := recover(); r != nil {
					fmt.Fprintf(os.Stderr, "[PANIC] processOrderBuffer (timer) para conta %d: %v\n", accountID, r)
				}
			}()
			wsm.processOrderBuffer(accountID, wsConn)
		})
		if logger != nil {
			logger.Log("[DEBUG] Primeira ordem recebida, iniciando timer de 2s para agrupamento")
		}
	} else {
		// Se já existe timer, resetar para mais 2 segundos
		if buffer.timer != nil {
			if !buffer.timer.Stop() {
				// Timer já foi executado, não fazer nada
			}
		}
		buffer.timer = time.AfterFunc(2*time.Second, func() {
			wsm.processOrderBuffer(accountID, wsConn)
		})
		if logger != nil {
			logger.Log("[DEBUG] Ordem adicional recebida, resetando timer de 2s (total: %d ordens)", len(buffer.orders))
		}
	}
}

func (wsm *WebSocketManager) addCancelToBuffer(accountID int64, order OrderData, wsConn *WebSocketConnection) {
	wsm.bufferMu.Lock()
	buffer, exists := wsm.cancelBuffers[accountID]
	if !exists {
		buffer = &CancelBuffer{
			orders:    []OrderData{},
			accountID: accountID,
		}
		wsm.cancelBuffers[accountID] = buffer
	}
	wsm.bufferMu.Unlock()

	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	// Adicionar cancelamento ao buffer
	buffer.orders = append(buffer.orders, order)

	// Resetar timer de 2 segundos a cada nova ordem cancelada
	if buffer.timer != nil {
		buffer.timer.Stop()
	}
	buffer.timer = time.AfterFunc(2*time.Second, func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "[PANIC] processCancelBuffer (timer) para conta %d: %v\n", accountID, r)
			}
		}()
		wsm.processCancelBuffer(accountID, wsConn)
	})
	logger, _ := getLogger(accountID, wsConn.Account.Name)
	if logger != nil {
		logger.Log("[DEBUG] Cancelamento recebido, resetando timer de 2s (total: %d cancelamentos)", len(buffer.orders))
	}
}

func (wsm *WebSocketManager) processOrderBuffer(accountID int64, wsConn *WebSocketConnection) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] processOrderBuffer para conta %d: %v\n", accountID, r)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(accountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em processOrderBuffer: %v", r)
				}
			}()
		}
	}()

	// Verificar se a conexão ainda está ativa
	wsm.mu.RLock()
	conn, exists := wsm.connections[accountID]
	if !exists || !conn.Running {
		wsm.mu.RUnlock()
		return
	}
	// Usar a conexão atual do mapa (pode ter mudado)
	activeConn := conn
	wsm.mu.RUnlock()

	wsm.bufferMu.Lock()
	buffer, exists := wsm.orderBuffers[accountID]
	if !exists {
		wsm.bufferMu.Unlock()
		return
	}

	buffer.mu.Lock()
	if len(buffer.orders) == 0 {
		buffer.mu.Unlock()
		wsm.bufferMu.Unlock()
		return
	}

	// Copiar ordens e limpar buffer
	orders := make([]OrderData, len(buffer.orders))
	copy(orders, buffer.orders)
	buffer.orders = []OrderData{} // Limpar buffer
	buffer.mu.Unlock()

	delete(wsm.orderBuffers, accountID)
	wsm.bufferMu.Unlock()

	if len(orders) == 0 {
		return
	}

	// Usar a conexão ativa
	wsConn = activeConn

	// Agrupar ordens por tipo (Side + OrderType + ReduceOnly)
	groups := make(map[string][]OrderData)
	for _, order := range orders {
		reducePrefix := ""
		if order.ReduceOnly {
			reducePrefix = "Reduce"
		}
		key := fmt.Sprintf("%s_%s_%s_%s", order.Symbol, reducePrefix, order.Side, order.OrderType)
		groups[key] = append(groups[key], order)
	}

	// Processar cada grupo
	for key, groupOrders := range groups {
		if len(groupOrders) == 0 {
			continue
		}

		// Pegar informações do primeiro para usar como base
		firstOrder := groupOrders[0]
		reducePrefix := ""
		if firstOrder.ReduceOnly {
			reducePrefix = "Reduce "
		}

		// Função auxiliar para obter o preço a ser exibido
		getDisplayPrice := func(order OrderData) string {
			// Se for Market e tiver avgPrice, usar avgPrice
			if order.OrderType == "Market" && order.AvgPrice != "" && order.AvgPrice != "0" {
				return order.AvgPrice
			}

			if order.OrderType == "Limit" && order.AvgPrice != "" && order.AvgPrice != "0" && (order.OrderStatus == "Filled" || order.OrderStatus == "PartiallyFilled") {
				return order.AvgPrice
			}
			// Caso contrário, usar Price
			return order.Price
		}

		// Calcular range de preços e quantidade total
		var minPrice, maxPrice float64
		var totalQty float64

		for i, order := range groupOrders {
			// Usar avgPrice se for Market, senão usar Price
			priceStr := getDisplayPrice(order)
			price, err := strconv.ParseFloat(priceStr, 64)
			if err != nil {
				// Erro ao parsear preço - pular esta ordem
				continue
			}
			qty, err := strconv.ParseFloat(order.Qty, 64)
			if err != nil {
				// Erro ao parsear quantidade - pular esta ordem
				continue
			}

			if i == 0 {
				minPrice = price
				maxPrice = price
			} else {
				if price < minPrice {
					minPrice = price
				}
				if price > maxPrice {
					maxPrice = price
				}
			}
			totalQty += qty
		}

		// Obter preço de exibição para a primeira ordem
		displayPrice := getDisplayPrice(firstOrder)

		// Construir mensagem
		var messageText string
		if len(groupOrders) == 1 {
			// Uma única ordem
			messageText = fmt.Sprintf("🟢 Nova ordem aberta - %s %s%s %s @ %s (Qty: %.2f USD)",
				firstOrder.Symbol, reducePrefix, firstOrder.Side, firstOrder.OrderType, displayPrice, totalQty)
		} else {
			// Múltiplas ordens agrupadas (scale orders)
			if minPrice == maxPrice {
				// Todas no mesmo preço
				messageText = fmt.Sprintf("🟢 %d ordens %s%s %s agrupadas - %s @ %s (Qty Total: %.2f USD)",
					len(groupOrders), reducePrefix, firstOrder.Side, firstOrder.OrderType, firstOrder.Symbol, displayPrice, totalQty)
			} else {
				// Range de preços
				messageText = fmt.Sprintf("🟢 %d ordens %s%s %s agrupadas - %s\n   Range: %.2f até %.2f\n   Qty Total: %.2f USD",
					len(groupOrders), reducePrefix, firstOrder.Side, firstOrder.OrderType, firstOrder.Symbol,
					minPrice, maxPrice, totalQty)
			}
		}

		// Enviar notificação
		wsm.sendNotification(wsConn, messageText)
		logger, _ := getLogger(accountID, wsConn.Account.Name)
		if logger != nil {
			logger.Log("[DEBUG] Processado grupo %s: %d ordens", key, len(groupOrders))
		}
	}
}

func (wsm *WebSocketManager) processCancelBuffer(accountID int64, wsConn *WebSocketConnection) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] processCancelBuffer para conta %d: %v\n", accountID, r)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(accountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em processCancelBuffer: %v", r)
				}
			}()
		}
	}()

	// Verificar se a conexão ainda está ativa
	wsm.mu.RLock()
	conn, exists := wsm.connections[accountID]
	if !exists || !conn.Running {
		wsm.mu.RUnlock()
		return
	}
	// Usar a conexão atual do mapa (pode ter mudado)
	activeConn := conn
	wsm.mu.RUnlock()

	wsm.bufferMu.Lock()
	buffer, exists := wsm.cancelBuffers[accountID]
	if !exists {
		wsm.bufferMu.Unlock()
		return
	}

	buffer.mu.Lock()
	if len(buffer.orders) == 0 {
		buffer.mu.Unlock()
		wsm.bufferMu.Unlock()
		return
	}

	// Copiar ordens e limpar buffer
	orders := make([]OrderData, len(buffer.orders))
	copy(orders, buffer.orders)
	buffer.orders = []OrderData{} // Limpar buffer
	buffer.mu.Unlock()

	delete(wsm.cancelBuffers, accountID)
	wsm.bufferMu.Unlock()

	if len(orders) == 0 {
		return
	}

	// Usar a conexão ativa
	wsConn = activeConn

	// Construir mensagem agrupada
	var messageParts []string
	messageParts = append(messageParts, fmt.Sprintf("❌ %d ordens canceladas:", len(orders)))

	for _, order := range orders {
		reducePrefix := ""
		if order.ReduceOnly {
			reducePrefix = "Reduce "
		}
		messageParts = append(messageParts, fmt.Sprintf("  • %s %s%s %s @ %s",
			order.Symbol, reducePrefix, order.Side, order.OrderType, order.Price))
	}

	messageText := strings.Join(messageParts, "\n")

	// Enviar notificação
	wsm.sendNotification(wsConn, messageText)
	logger, _ := getLogger(accountID, wsConn.Account.Name)
	if logger != nil {
		logger.Log("[DEBUG] Processado %d cancelamentos agrupados", len(orders))
	}
}

func (wsm *WebSocketManager) processStopOrder(wsConn *WebSocketConnection, order OrderData) {
	// Processar stop order imediatamente (sem delay)
	reducePrefix := ""
	if order.ReduceOnly {
		reducePrefix = "Reduce "
	}

	// Converter triggerPrice e qty para float para formatação
	triggerPrice, err := strconv.ParseFloat(order.TriggerPrice, 64)
	if err != nil {
		triggerPrice = 0
	}

	qty, err := strconv.ParseFloat(order.Qty, 64)
	if err != nil {
		qty = 0
	}

	// Escolher ícone baseado no Side (Buy = verde, Sell = vermelho)
	var stopIcon string
	if order.Side == "Buy" {
		stopIcon = "🟢" // Círculo verde para Buy
	} else {
		stopIcon = "🔴" // Círculo vermelho para Sell
	}

	// Formatar mensagem do stop
	messageText := fmt.Sprintf("%s Stop %s%s %s - %s @ %.2f (Qty: %.2f USD)",
		stopIcon, reducePrefix, order.Side, order.OrderType, order.Symbol, triggerPrice, qty)

	// Enviar notificação imediatamente
	wsm.sendNotification(wsConn, messageText)
	logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
	if logger != nil {
		logger.Log("[DEBUG] Stop order processado - %s %s%s @ %.2f", order.Symbol, reducePrefix, order.Side, triggerPrice)
	}
}

func (wsm *WebSocketManager) processStopCancellation(wsConn *WebSocketConnection, order OrderData) {
	// Processar cancelamento de stop não triggerado imediatamente (sem delay)
	reducePrefix := ""
	if order.ReduceOnly {
		reducePrefix = "Reduce "
	}

	// Converter triggerPrice e qty para float para formatação
	triggerPrice, err := strconv.ParseFloat(order.TriggerPrice, 64)
	if err != nil {
		triggerPrice = 0
	}

	qty, err := strconv.ParseFloat(order.Qty, 64)
	if err != nil {
		qty = 0
	}

	// Escolher ícone baseado no Side (Buy = verde, Sell = vermelho)
	var stopIcon string
	if order.Side == "Buy" {
		stopIcon = "🟢" // Círculo verde para Buy
	} else {
		stopIcon = "🔴" // Círculo vermelho para Sell
	}

	// Formatar mensagem do cancelamento de stop
	messageText := fmt.Sprintf("❌ %s Stop %s%s %s **CANCELADO** - %s @ %.2f (Qty: %.2f USD)",
		stopIcon, reducePrefix, order.Side, order.OrderType, order.Symbol, triggerPrice, qty)

	// Enviar notificação imediatamente
	wsm.sendNotification(wsConn, messageText)
	logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
	if logger != nil {
		logger.Log("[DEBUG] Stop cancelado processado - %s %s%s @ %.2f", order.Symbol, reducePrefix, order.Side, triggerPrice)
	}
}

func (wsm *WebSocketManager) handleExecutionMessage(wsConn *WebSocketConnection, execMsg BybitExecutionMessage) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] handleExecutionMessage para conta %d: %v\n", wsConn.AccountID, r)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em handleExecutionMessage: %v", r)
				}
			}()
		}
	}()

	logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)

	if logger != nil {
		logger.Log("[DEBUG] Mensagem de execution recebida! Total de execuções: %d", len(execMsg.Data))
	}

	for _, execData := range execMsg.Data {
		// Processar apenas execuções inverse
		if execData.Category != "inverse" {
			if logger != nil {
				logger.Log("[DEBUG] Execução ignorada - não é inverse (category: %s)", execData.Category)
			}
			continue
		}

		// Processar apenas execuções do tipo Trade
		if execData.ExecType != "Trade" {
			if logger != nil {
				logger.Log("[DEBUG] Execução ignorada - não é Trade (execType: %s)", execData.ExecType)
			}
			continue
		}

		if logger != nil {
			logger.Log("[DEBUG] Processando execução - Symbol: %s, Side: %s, ExecPrice: %s",
				execData.Symbol, execData.Side, execData.ExecPrice)
		}

		// Adicionar ao buffer de execution (inicia/reseta timer de 5 minutos)
		wsm.addExecutionToBuffer(wsConn.AccountID, wsConn)
	}
}

func (wsm *WebSocketManager) handlePositionMessage(wsConn *WebSocketConnection, posMsg BybitPositionMessage) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] handlePositionMessage para conta %d: %v\n", wsConn.AccountID, r)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em handlePositionMessage: %v", r)
				}
			}()
		}
	}()

	logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)

	if logger != nil {
		logger.Log("[DEBUG] Mensagem de position recebida! Total de posições: %d", len(posMsg.Data))
	}

	// Processar apenas posições inverse
	for _, posData := range posMsg.Data {
		if posData.Category != "inverse" {
			if logger != nil {
				logger.Log("[DEBUG] Posição ignorada - não é inverse (category: %s)", posData.Category)
			}
			continue
		}

		// Atualizar posição por símbolo no buffer de execution (se existir)
		wsm.bufferMu.Lock()
		if buffer, exists := wsm.executionBuffers[wsConn.AccountID]; exists {
			buffer.mu.Lock()
			// Inicializar mapa se necessário
			if buffer.positions == nil {
				buffer.positions = make(map[string]*PositionData)
			}
			// Criar cópia da posição e armazenar por símbolo
			posCopy := posData
			buffer.positions[posData.Symbol] = &posCopy
			buffer.mu.Unlock()
			if logger != nil {
				logger.Log("[DEBUG] Posição atualizada no buffer para símbolo: %s", posData.Symbol)
			}
		}
		wsm.bufferMu.Unlock()
	}
}

func (wsm *WebSocketManager) handleWalletMessage(wsConn *WebSocketConnection, walletMsg BybitWalletMessage) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] handleWalletMessage para conta %d: %v\n", wsConn.AccountID, r)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em handleWalletMessage: %v", r)
				}
			}()
		}
	}()

	logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)

	if logger != nil {
		logger.Log("[DEBUG] Mensagem de wallet recebida! Total de wallets: %d", len(walletMsg.Data))
	}

	// Processar apenas wallets UNIFIED
	for _, walletData := range walletMsg.Data {
		if walletData.AccountType != "UNIFIED" {
			if logger != nil {
				logger.Log("[DEBUG] Wallet ignorado - não é UNIFIED (accountType: %s)", walletData.AccountType)
			}
			continue
		}

		// Atualizar último wallet no buffer de execution (se existir)
		wsm.bufferMu.Lock()
		if buffer, exists := wsm.executionBuffers[wsConn.AccountID]; exists {
			buffer.mu.Lock()
			// Criar cópia do wallet
			walletCopy := walletData
			
			// Se já existe uma wallet no buffer, fazer merge das coins
			if buffer.lastWallet != nil {
				// Criar mapa das coins novas (por nome da coin) para busca rápida
				newCoinsMap := make(map[string]CoinBalance)
				for _, coin := range walletData.Coin {
					newCoinsMap[coin.Coin] = coin
				}
				
				// Criar slice para o resultado do merge
				mergedCoins := make([]CoinBalance, 0)
				
				// Primeiro, adicionar coins antigas que não existem nas novas
				for _, oldCoin := range buffer.lastWallet.Coin {
					if _, exists := newCoinsMap[oldCoin.Coin]; !exists {
						mergedCoins = append(mergedCoins, oldCoin)
					}
				}
				
				// Depois, adicionar todas as coins novas (que sobrescrevem as antigas)
				for _, newCoin := range walletData.Coin {
					mergedCoins = append(mergedCoins, newCoin)
				}
				
				// Atualizar o array de coins no walletCopy com o resultado do merge
				walletCopy.Coin = mergedCoins
				
				if logger != nil {
					logger.Log("[DEBUG] Wallet mesclado: %d coins antigas mantidas, %d coins novas adicionadas", 
						len(buffer.lastWallet.Coin)-len(walletData.Coin), len(walletData.Coin))
				}
			} else {
				if logger != nil {
					logger.Log("[DEBUG] Primeira wallet adicionada ao buffer")
				}
			}
			
			buffer.lastWallet = &walletCopy
			buffer.mu.Unlock()
			if logger != nil {
				logger.Log("[DEBUG] Último wallet atualizado no buffer")
			}
		}
		wsm.bufferMu.Unlock()
	}
}

func (wsm *WebSocketManager) addExecutionToBuffer(accountID int64, wsConn *WebSocketConnection) {
	wsm.bufferMu.Lock()
	buffer, exists := wsm.executionBuffers[accountID]
	if !exists {
		buffer = &ExecutionBuffer{
			accountID: accountID,
			positions: make(map[string]*PositionData),
		}
		wsm.executionBuffers[accountID] = buffer
	}
	wsm.bufferMu.Unlock()

	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	// Garantir que o mapa de posições está inicializado
	if buffer.positions == nil {
		buffer.positions = make(map[string]*PositionData)
	}

	// Se já existe timer, resetar para mais 5 minutos
	if buffer.timer != nil {
		buffer.timer.Stop()
	}

	// Iniciar/resetar timer de 5 minutos
	buffer.timer = time.AfterFunc(5*time.Minute, func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Fprintf(os.Stderr, "[PANIC] processExecutionBuffer (timer) para conta %d: %v\n", accountID, r)
			}
		}()
		wsm.processExecutionBuffer(accountID, wsConn)
	})
	logger, _ := getLogger(accountID, wsConn.Account.Name)
	if logger != nil {
		logger.Log("[DEBUG] Execução recebida, iniciando/resetando timer de 5 minutos")
	}
}

func (wsm *WebSocketManager) processExecutionBuffer(accountID int64, wsConn *WebSocketConnection) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] processExecutionBuffer para conta %d: %v\n", accountID, r)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(accountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em processExecutionBuffer: %v", r)
				}
			}()
		}
	}()

	// Verificar se a conexão ainda está ativa
	wsm.mu.RLock()
	conn, exists := wsm.connections[accountID]
	if !exists || !conn.Running {
		wsm.mu.RUnlock()
		return
	}
	activeConn := conn
	wsm.mu.RUnlock()

	wsm.bufferMu.Lock()
	buffer, exists := wsm.executionBuffers[accountID]
	if !exists {
		wsm.bufferMu.Unlock()
		return
	}

	buffer.mu.Lock()
	lastWallet := buffer.lastWallet
	positions := make(map[string]*PositionData)
	// Copiar posições
	if buffer.positions != nil {
		for symbol, pos := range buffer.positions {
			positions[symbol] = pos
		}
	}
	buffer.mu.Unlock()

	// Não remover o buffer ainda, apenas processar
	wsm.bufferMu.Unlock()

	// Usar a conexão ativa
	wsConn = activeConn

	// Verificar se temos wallet
	if lastWallet == nil {
		// Nenhum wallet disponível - não processar
		return
	}

	// Obter valor total da carteira do wallet
	totalEquity, err := strconv.ParseFloat(lastWallet.TotalEquity, 64)
	if err != nil {
		// Fallback para totalWalletBalance
		totalEquity, err = strconv.ParseFloat(lastWallet.TotalWalletBalance, 64)
		if err != nil {
			// Não foi possível obter valor da carteira - não processar
			return
		}
	}

	// Função auxiliar para calcular proteção de uma posição
	calculatePositionValues := func(position *PositionData, totalEquityCoin float64) (longUSD, protecaoUSD, expostoUSD float64) {
		// Converter position.Size de string para float64
		size, err := strconv.ParseFloat(position.Size, 64)
		if err != nil {
			size = 0
		}

		if position.Side == "Sell" {
			protecaoUSD = size
			longUSD = 0
		} else {
			protecaoUSD = 0
			longUSD = size
		}

		// Exposto = total da moeda - protegido
		expostoUSD = totalEquityCoin - size
		return
	}

	// Calcular valores por moeda e preparar mensagens
	var totalProtecaoUSD float64
	var totalLongUSD float64
	var totalExposicaoUSD float64
	var coinMessages []string // Mensagens por moeda para usar no else se necessário
	var totalValidPositions int = 0

	// Processar todas as posições para calcular totais e criar mensagens
	for symbol, position := range positions {
		// Extrair moeda do símbolo (ex: BTCUSD -> BTC, ETHUSD -> ETH)
		coin := symbol
		if strings.HasSuffix(symbol, "USD") {
			coin = symbol[:len(symbol)-3]
		} else if strings.HasSuffix(symbol, "USDT") {
			coin = symbol[:len(symbol)-4]
		} else if strings.HasSuffix(symbol, "USDC") {
			coin = symbol[:len(symbol)-4]
		}

		var totalEquityPerCoin float64
		for _, coinBalance := range lastWallet.Coin {
			if coinBalance.Coin == coin {
				equity, err := strconv.ParseFloat(coinBalance.UsdValue, 64)
				if err == nil {
					totalEquityPerCoin = equity
				}
				break
			}
		}

		// ignorar moedas com balance inferior a 10 USD
		if (totalEquityPerCoin < 10) {
			continue
		}

		totalValidPositions++

		// Calcular valores da posição
		longPosUSD, protecaoPosUSD, expostoPosUSD := calculatePositionValues(position, totalEquityPerCoin)
		totalProtecaoUSD += protecaoPosUSD
		totalLongUSD += longPosUSD
		totalExposicaoUSD += expostoPosUSD

		// Calcular % protegida para esta posição
		var percentProtegidaPos float64 = 0.0
		if protecaoPosUSD > 0 && totalEquityPerCoin > 0 {
			percentProtegidaPos = (protecaoPosUSD / totalEquityPerCoin) * 100
		}

		// Calcular % longada para esta posição
		var percentLongadaPos float64 = 0.0
		if longPosUSD > 0 && totalEquityPerCoin > 0 {
			percentLongadaPos = (longPosUSD / totalEquityPerCoin) * 100
		}

		// Criar mensagem da moeda (para usar no else se necessário)
		var coinMsgParts []string
		coinMsgParts = append(coinMsgParts, fmt.Sprintf("📌 %s (%s):", coin, symbol))
		coinMsgParts = append(coinMsgParts, fmt.Sprintf("  💰 Total: $%.2f USD", totalEquityPerCoin))
		coinMsgParts = append(coinMsgParts, fmt.Sprintf("  🛡️ Protegido: $%.2f USD", protecaoPosUSD))
		if longPosUSD > 0 {
			coinMsgParts = append(coinMsgParts, fmt.Sprintf("  📈 Posição Long: $%.2f USD", longPosUSD))
		}
		coinMsgParts = append(coinMsgParts, fmt.Sprintf("  ⚠️ Exposto: $%.2f USD", expostoPosUSD))
		coinMsgParts = append(coinMsgParts, fmt.Sprintf("  📈 %% Protegida: %.2f%%", percentProtegidaPos))
		if longPosUSD > 0 {
			coinMsgParts = append(coinMsgParts, fmt.Sprintf("  📊 %% Longada: %.2f%%", percentLongadaPos))
		}
		coinMsgParts = append(coinMsgParts, "")
		coinMessages = append(coinMessages, strings.Join(coinMsgParts, "\n"))
	}

	// Construir mensagem
	var messageParts []string

	for _, coinMsg := range coinMessages {
		messageParts = append(messageParts, coinMsg)
	}

	// retornar o resumo geral da carteira apenas se tiver mais de uma posição válida ou nenhuma posição válida
	if totalValidPositions != 1 {
		messageParts = append(messageParts, "📊 Resumo Geral:")
		messageParts = append(messageParts, fmt.Sprintf("  💰 Carteira Total: $%.2f USD", totalEquity))
		messageParts = append(messageParts, fmt.Sprintf("  🛡️ Proteção Total: $%.2f USD", totalProtecaoUSD))
		if totalLongUSD > 0 {
			messageParts = append(messageParts, fmt.Sprintf("  📈 Long Total: $%.2f USD", totalLongUSD))
		}
		messageParts = append(messageParts, fmt.Sprintf("  ⚠️ Exposição Total: $%.2f USD", totalExposicaoUSD))

		// Calcular % protegida geral
		var percentProtegidaGeral float64
		if totalEquity > 0 {
			percentProtegidaGeral = (totalProtecaoUSD / totalEquity) * 100
		}
		messageParts = append(messageParts, fmt.Sprintf("  📈 %% Protegida: %.2f%%", percentProtegidaGeral))
	
		// Calcular % longada geral
		if totalLongUSD > 0 {
			var percentLongadaGeral float64
			if totalEquity > 0 {
				percentLongadaGeral = (totalLongUSD / totalEquity) * 100
			}
			messageParts = append(messageParts, fmt.Sprintf("  📊 %% Longada: %.2f%%", percentLongadaGeral))
		}
	}

	messageText := strings.Join(messageParts, "\n")

	// Enviar notificação
	wsm.sendNotification(wsConn, messageText)
	logger, _ := getLogger(accountID, wsConn.Account.Name)
	if logger != nil {
		logger.Log("[DEBUG] Notificação de posição enviada após 5 minutos sem execuções")
	}
}

func (wsm *WebSocketManager) sendNotification(wsConn *WebSocketConnection, messageText string) {
	// Capturar panics
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[PANIC] sendNotification para conta %d: %v\n", wsConn.AccountID, r)
			func() {
				defer func() {
					if r2 := recover(); r2 != nil {
						fmt.Fprintf(os.Stderr, "ERRO ao tentar logar panic: %v\n", r2)
					}
				}()
				logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
				if logger != nil {
					logger.Log("PANIC em sendNotification: %v", r)
				}
			}()
		}
	}()

	logger, _ := getLogger(wsConn.AccountID, wsConn.Account.Name)
	
	alertIcon := "🔔" // Altere aqui para escolher outro ícone
	
	// Obter data/hora atual no horário de Brasília (funciona no Windows e Linux)
	now := getBrasiliaTime()
	timeStamp := fmt.Sprintf("🕘  %s - %s (Horário de Brasília)",
		now.Format("02/01/2006"),
		now.Format("15:04"))
	
	if wsConn.Account.WebhookURL != "" {
		// Enviar para Discord
		// Discord remove quebras de linha no início, então precisamos ter conteúdo antes
		discordMsg := fmt.Sprintf("%s\n%s\n\n%s", alertIcon, messageText, timeStamp)
		if err := sendDiscordWebhook(wsConn.Account.WebhookURL, discordMsg); err != nil {
			// Fallback para logger em caso de erro no webhook
			if logger != nil {
				logger.Log("Erro ao enviar webhook, notificação: %s", messageText)
			}
		}
		// Não logar quando enviado com sucesso para evitar logs desnecessários
	}
	// Quando não há webhook, não fazer nada (não logar nem imprimir)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func sendDiscordWebhook(webhookURL, message string) error {
	payload := map[string]string{
		"content": message,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := http.Post(webhookURL, "application/json", bytes.NewReader(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status code: %d", resp.StatusCode)
	}

	return nil
}
