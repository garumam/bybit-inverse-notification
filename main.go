package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
)

func main() {
	db, err := NewDatabase()
	if err != nil {
		fmt.Printf("Erro ao conectar ao banco de dados: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	manager := NewAccountManager(db)
	wsManager := NewWebSocketManager(db, manager)

	// Restaurar conexões ativas ao iniciar
	if err := wsManager.RestoreConnections(); err != nil {
		fmt.Printf("Erro ao restaurar conexões: %v\n", err)
	}

	scanner := bufio.NewScanner(os.Stdin)

	for {
		clearScreen()
		showMenu(wsManager)
		fmt.Print("Escolha uma opção: ")
		scanner.Scan()
		choice := strings.TrimSpace(scanner.Text())

		switch choice {
		case "1":
			handleAddAccount(manager, scanner)
		case "2":
			handleListAccounts(manager, wsManager, scanner)
		case "3":
			handleRemoveAccount(manager, wsManager, scanner)
		case "4":
			handleEditAccount(manager, wsManager, scanner)
		case "5":
			handleStartWebSocket(wsManager, scanner)
		case "6":
			handleStopWebSocket(wsManager, scanner)
		case "7":
			handleViewMonitoredAccounts(wsManager, scanner)
		case "8":
			handleViewLogs(wsManager.accountManager, scanner)
		case "9":
			fmt.Println("Saindo...")
			wsManager.StopAll()
			return
		default:
		}
	}
}

func clearScreen() {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.Command("cmd", "/c", "cls")
	} else {
		cmd = exec.Command("clear")
	}
	cmd.Stdout = os.Stdout
	cmd.Run()
}

func getMonitoredAccountsCount(wsManager *WebSocketManager) int {
	accounts, err := wsManager.accountManager.ListAccounts()
	if err != nil {
		return 0
	}

	count := 0
	for _, acc := range accounts {
		if wsManager.IsConnectionActive(acc.ID) {
			count++
		}
	}
	return count
}

func showMenu(wsManager *WebSocketManager) {
	monitoredCount := getMonitoredAccountsCount(wsManager)
	
	fmt.Println("\n=== Gerenciador de Contas Bybit ===")
	fmt.Printf("📊 Contas sendo monitoradas: %d\n", monitoredCount)
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("1. Cadastrar conta Bybit")
	fmt.Println("2. Listar contas cadastradas")
	fmt.Println("3. Remover conta cadastrada")
	fmt.Println("4. Editar conta")
	fmt.Println("5. Monitorar conta")
	fmt.Println("6. Parar monitoramento da conta")
	fmt.Println("7. Ver contas monitoradas")
	fmt.Println("8. Visualizar logs")
	fmt.Println("9. Desligar")
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("ℹ️  Se a janela for fechada, o monitoramento será pausado")
	fmt.Println("   automaticamente.")
	fmt.Println()
}

func handleAddAccount(manager *AccountManager, scanner *bufio.Scanner) {
	clearScreen()
	fmt.Println("=== Cadastrar Conta Bybit ===")
	fmt.Println("(Digite 'cancelar' ou '0' em qualquer momento para voltar ao menu principal)\n")
	
	fmt.Print("Nome da conta: ")
	scanner.Scan()
	nome := strings.TrimSpace(scanner.Text())
	if nome == "cancelar" || nome == "0" {
		return
	}

	fmt.Print("API Key: ")
	scanner.Scan()
	apiKey := strings.TrimSpace(scanner.Text())
	if apiKey == "cancelar" || apiKey == "0" {
		return
	}

	fmt.Print("API Secret: ")
	scanner.Scan()
	apiSecret := strings.TrimSpace(scanner.Text())
	if apiSecret == "cancelar" || apiSecret == "0" {
		return
	}

	fmt.Print("Webhook Discord (opcional, deixe em branco para notificar no terminal): ")
	scanner.Scan()
	webhookURL := strings.TrimSpace(scanner.Text())
	if webhookURL == "cancelar" || webhookURL == "0" {
		return
	}

	if nome == "" || apiKey == "" || apiSecret == "" {
		fmt.Println("Erro: Nome, API Key e API Secret são obrigatórios!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	account := &BybitAccount{
		Name:       nome,
		APIKey:     apiKey,
		APISecret:  apiSecret,
		WebhookURL: webhookURL,
		Active:     true,
	}

	if err := manager.AddAccount(account); err != nil {
		fmt.Printf("Erro ao cadastrar conta: %v\n", err)
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
	} else {
		fmt.Println("Conta cadastrada com sucesso!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
	}
}

func handleListAccounts(manager *AccountManager, wsManager *WebSocketManager, scanner *bufio.Scanner) {
	clearScreen()
	accounts, err := manager.ListAccounts()
	if err != nil {
		fmt.Printf("Erro ao listar contas: %v\n", err)
		return
	}

	fmt.Println("\n=== Contas Cadastradas ===")
	if len(accounts) == 0 {
		fmt.Println("Nenhuma conta cadastrada.")
	} else {
		for i, acc := range accounts {
			monitoringStatus := "Desligado"
			if wsManager.IsConnectionActive(acc.ID) {
				monitoringStatus = "Ligado"
			}
			
			fmt.Printf("\n%d. Nome: %s\n", i+1, acc.Name)
			fmt.Printf("   API Key: %s\n", maskAPIKey(acc.APIKey))
			if acc.WebhookURL != "" {
				fmt.Printf("   Webhook Discord: Configurado\n")
			} else {
				fmt.Printf("   Webhook Discord: Não configurado (notificações no terminal)\n")
			}
			fmt.Printf("   Status: %s\n", getStatusText(acc.Active))
			fmt.Printf("   Monitoramento: %s\n", monitoringStatus)
		}
		fmt.Printf("\nTotal: %d conta(s) cadastrada(s)\n", len(accounts))
	}
	
	fmt.Println("\nPressione Enter para voltar ao menu principal...")
	scanner.Scan()
}

func handleRemoveAccount(manager *AccountManager, wsManager *WebSocketManager, scanner *bufio.Scanner) {
	clearScreen()
	accounts, err := manager.ListAccounts()
	if err != nil {
		fmt.Printf("Erro ao listar contas: %v\n", err)
		return
	}

	if len(accounts) == 0 {
		fmt.Println("Nenhuma conta cadastrada.")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	fmt.Println("\n=== Contas Cadastradas ===")
	for i, acc := range accounts {
		fmt.Printf("%d. %s\n", i+1, acc.Name)
	}
	fmt.Println("0. Voltar ao menu principal")

	fmt.Print("\nDigite o número da conta para remover (ou 0 para voltar): ")
	scanner.Scan()
	var index int
	if _, err := fmt.Sscanf(scanner.Text(), "%d", &index); err != nil {
		fmt.Println("Número inválido!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	if index == 0 {
		return
	}

	if index < 1 || index > len(accounts) {
		fmt.Println("Número inválido!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	account := accounts[index-1]
	
	// Verificar se a conta está sendo monitorada
	if wsManager.IsConnectionActive(account.ID) {
		clearScreen()
		fmt.Println("═══════════════════════════════════════════════════════════")
		fmt.Println("  ERRO: Não é possível remover a conta enquanto está")
		fmt.Println("        sendo monitorada!")
		fmt.Println("═══════════════════════════════════════════════════════════")
		fmt.Printf("\nA conta '%s' está sendo monitorada no momento.\n", account.Name)
		fmt.Println("\nPor favor:")
		fmt.Println("1. Volte ao menu principal")
		fmt.Println("2. Escolha a opção '5. Parar monitoramento da conta'")
		fmt.Println("3. Pare o monitoramento desta conta")
		fmt.Println("4. Depois tente remover novamente")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	// Pedir confirmação antes de remover
	clearScreen()
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Printf("  ATENÇÃO: Você está prestes a remover a conta '%s'\n", account.Name)
	fmt.Println("═══════════════════════════════════════════════════════════")
	fmt.Println("\nEsta ação não pode ser desfeita!")
	fmt.Print("\nDeseja realmente remover esta conta? (sim/s ou não/n): ")
	scanner.Scan()
	confirmation := strings.ToLower(strings.TrimSpace(scanner.Text()))

	if confirmation != "sim" && confirmation != "s" {
		fmt.Println("\nRemoção cancelada.")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	// Remover a conta
	if err := manager.RemoveAccount(account.ID); err != nil {
		fmt.Printf("\nErro ao remover conta: %v\n", err)
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
	} else {
		fmt.Printf("\nConta '%s' removida com sucesso!\n", account.Name)
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
	}
}

func handleEditAccount(manager *AccountManager, wsManager *WebSocketManager, scanner *bufio.Scanner) {
	clearScreen()
	accounts, err := manager.ListAccounts()
	if err != nil {
		fmt.Printf("Erro ao listar contas: %v\n", err)
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	if len(accounts) == 0 {
		fmt.Println("Nenhuma conta cadastrada.")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	fmt.Println("\n=== Contas Cadastradas ===")
	for i, acc := range accounts {
		fmt.Printf("%d. %s\n", i+1, acc.Name)
	}
	fmt.Println("0. Voltar ao menu principal")

	fmt.Print("\nDigite o número da conta para editar (ou 0 para voltar): ")
	scanner.Scan()
	var index int
	if _, err := fmt.Sscanf(scanner.Text(), "%d", &index); err != nil {
		fmt.Println("Número inválido!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	if index == 0 {
		return
	}

	if index < 1 || index > len(accounts) {
		fmt.Println("Número inválido!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	account := accounts[index-1]
	
	clearScreen()
	fmt.Println("=== Editar Conta ===")
	fmt.Printf("Conta: %s\n", account.Name)
	fmt.Println("(Digite 'cancelar' ou '0' em qualquer momento para voltar ao menu principal)\n")
	
	// Mostrar valores atuais
	fmt.Printf("Nome atual: %s\n", account.Name)
	fmt.Print("Novo nome (pressione Enter para manter o atual): ")
	scanner.Scan()
	newName := strings.TrimSpace(scanner.Text())
	if newName == "cancelar" || newName == "0" {
		return
	}
	if newName == "" {
		newName = account.Name
	}

	currentWebhook := account.WebhookURL
	if currentWebhook == "" {
		currentWebhook = "(não configurado)"
	}
	fmt.Printf("\nWebhook Discord atual: %s\n", currentWebhook)
	fmt.Print("Novo Webhook Discord (pressione Enter para manter o atual, ou digite 'remover' para remover): ")
	scanner.Scan()
	newWebhook := strings.TrimSpace(scanner.Text())
	if newWebhook == "cancelar" || newWebhook == "0" {
		return
	}
	if newWebhook == "" {
		newWebhook = account.WebhookURL
	} else if newWebhook == "remover" {
		newWebhook = ""
	}

	// Verificar se a conta está sendo monitorada antes de editar
	wasMonitored := wsManager.IsConnectionActive(account.ID)

	// Atualizar conta
	if err := manager.UpdateAccount(account.ID, newName, newWebhook); err != nil {
		fmt.Printf("\nErro ao editar conta: %v\n", err)
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
	} else {
		fmt.Println("\nConta editada com sucesso!")
		
		// Se a conta estava sendo monitorada, reiniciar o monitoramento
		if wasMonitored {
			fmt.Println("\nReiniciando monitoramento para aplicar as alterações...")
			// Parar o monitoramento atual
			wsManager.StopConnection(account.ID)
			// Aguardar um pouco para garantir que a conexão foi fechada
			// Reiniciar o monitoramento com os dados atualizados
			if err := wsManager.StartConnection(account.ID); err != nil {
				fmt.Printf("Aviso: Erro ao reiniciar monitoramento: %v\n", err)
				fmt.Println("Por favor, reinicie o monitoramento manualmente.")
			} else {
				fmt.Println("Monitoramento reiniciado com sucesso!")
			}
		}
		
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
	}
}

func handleStartWebSocket(wsManager *WebSocketManager, scanner *bufio.Scanner) {
	clearScreen()
	accounts, err := wsManager.accountManager.ListAccounts()
	if err != nil {
		fmt.Printf("Erro ao listar contas: %v\n", err)
		return
	}

	if len(accounts) == 0 {
		fmt.Println("Nenhuma conta cadastrada.")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	fmt.Println("\n=== Contas Cadastradas ===")
	for i, acc := range accounts {
		fmt.Printf("%d. %s\n", i+1, acc.Name)
	}
	fmt.Printf("%d. Todas as contas\n", len(accounts)+1)
	fmt.Println("0. Voltar ao menu principal")

	fmt.Print("\nDigite o número da conta para monitorar (ou 0 para voltar): ")
	scanner.Scan()
	input := strings.TrimSpace(scanner.Text())
	var index int
	if _, err := fmt.Sscanf(input, "%d", &index); err != nil {
		fmt.Println("Número inválido!")
		return
	}

	if index == 0 {
		return
	}

	if index == len(accounts)+1 {
		// Iniciar todas as contas
		if err := wsManager.StartAllConnections(); err != nil {
			fmt.Printf("Erro ao iniciar monitoramento: %v\n", err)
			fmt.Println("\nPressione Enter para voltar ao menu principal...")
			scanner.Scan()
		} else {
			fmt.Println("Todas as contas estão sendo monitoradas!")
			fmt.Println("\nPressione Enter para ver as contas monitoradas...")
			scanner.Scan()
			handleViewMonitoredAccounts(wsManager, scanner)
		}
	} else if index >= 1 && index <= len(accounts) {
		// Iniciar conta específica
		account := accounts[index-1]
		if err := wsManager.StartConnection(account.ID); err != nil {
			fmt.Printf("Erro ao iniciar monitoramento: %v\n", err)
			fmt.Println("\nPressione Enter para voltar ao menu principal...")
			scanner.Scan()
		} else {
			fmt.Printf("Monitoramento iniciado para conta '%s'!\n", account.Name)
			fmt.Println("\nPressione Enter para ver as contas monitoradas...")
			scanner.Scan()
			handleViewMonitoredAccounts(wsManager, scanner)
		}
	} else {
		fmt.Println("Número inválido!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
	}
}

func handleStartAllWebSockets(wsManager *WebSocketManager) {
	if err := wsManager.StartAllConnections(); err != nil {
		fmt.Printf("Erro ao iniciar WebSockets: %v\n", err)
	} else {
		fmt.Println("Todos os WebSockets iniciados!")
	}
}

func handleStopWebSocket(wsManager *WebSocketManager, scanner *bufio.Scanner) {
	clearScreen()
	accounts, err := wsManager.accountManager.ListAccounts()
	if err != nil {
		fmt.Printf("Erro ao listar contas: %v\n", err)
		return
	}

	activeAccounts := []*BybitAccount{}
	for _, acc := range accounts {
		if wsManager.IsConnectionActive(acc.ID) {
			activeAccounts = append(activeAccounts, acc)
		}
	}

	if len(activeAccounts) == 0 {
		fmt.Println("Nenhuma conexão ativa.")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	fmt.Println("\n=== Conexões Ativas ===")
	for i, acc := range activeAccounts {
		fmt.Printf("%d. %s\n", i+1, acc.Name)
	}
	fmt.Printf("%d. Todas as contas\n", len(activeAccounts)+1)
	fmt.Println("0. Voltar ao menu principal")

	fmt.Print("\nDigite o número da conta para parar monitoramento (ou 0 para voltar): ")
	scanner.Scan()
	input := strings.TrimSpace(scanner.Text())
	var index int
	if _, err := fmt.Sscanf(input, "%d", &index); err != nil {
		fmt.Println("Número inválido!")
		return
	}

	if index == 0 {
		return
	}

	if index == len(activeAccounts)+1 {
		// Parar todas as contas
		wsManager.StopAll()
		fmt.Println("Monitoramento de todas as contas parado!")
		fmt.Println("\nPressione Enter para ver as contas monitoradas...")
		scanner.Scan()
		handleViewMonitoredAccounts(wsManager, scanner)
	} else if index >= 1 && index <= len(activeAccounts) {
		// Parar conta específica
		account := activeAccounts[index-1]
		wsManager.StopConnection(account.ID)
		fmt.Printf("Monitoramento parado para conta '%s'!\n", account.Name)
		fmt.Println("\nPressione Enter para ver as contas monitoradas...")
		scanner.Scan()
		handleViewMonitoredAccounts(wsManager, scanner)
	} else {
		fmt.Println("Número inválido!")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
	}
}

func handleStopAllWebSockets(wsManager *WebSocketManager) {
	wsManager.StopAll()
	fmt.Println("Todos os WebSockets parados!")
}

func maskAPIKey(key string) string {
	if len(key) <= 8 {
		return "****"
	}
	return key[:4] + "****" + key[len(key)-4:]
}

func getStatusText(active bool) string {
	if active {
		return "Ativa"
	}
	return "Inativa"
}

func handleViewMonitoredAccounts(wsManager *WebSocketManager, scanner *bufio.Scanner) {
	clearScreen()
	accounts, err := wsManager.accountManager.ListAccounts()
	if err != nil {
		fmt.Printf("Erro ao listar contas: %v\n", err)
		return
	}

	monitoredAccounts := []*BybitAccount{}
	for _, acc := range accounts {
		if wsManager.IsConnectionActive(acc.ID) {
			monitoredAccounts = append(monitoredAccounts, acc)
		}
	}

	fmt.Println("\n=== Contas Monitoradas (WebSocket Ativo) ===")
	if len(monitoredAccounts) == 0 {
		fmt.Println("Nenhuma conta está sendo monitorada no momento.")
	} else {
		for i, acc := range monitoredAccounts {
			fmt.Printf("\n%d. Nome: %s\n", i+1, acc.Name)
			fmt.Printf("   API Key: %s\n", maskAPIKey(acc.APIKey))
			if acc.WebhookURL != "" {
				fmt.Printf("   Webhook Discord: Configurado\n")
			} else {
				fmt.Printf("   Webhook Discord: Não configurado (notificações no terminal)\n")
			}
			fmt.Printf("   Status: Monitorando\n")
		}
		fmt.Printf("\nTotal: %d conta(s) sendo monitorada(s)\n", len(monitoredAccounts))
	}
	
	fmt.Println("\nPressione Enter para voltar ao menu principal...")
	scanner.Scan()
}

func handleViewLogs(manager *AccountManager, scanner *bufio.Scanner) {
	clearScreen()
	accounts, err := manager.ListAccounts()
	if err != nil {
		fmt.Printf("Erro ao listar contas: %v\n", err)
		return
	}

	if len(accounts) == 0 {
		fmt.Println("Nenhuma conta cadastrada.")
		fmt.Println("\nPressione Enter para voltar ao menu principal...")
		scanner.Scan()
		return
	}

	fmt.Println("\n=== Contas Cadastradas ===")
	for i, acc := range accounts {
		fmt.Printf("%d. %s\n", i+1, acc.Name)
	}
	fmt.Println("0. Voltar ao menu principal")

	fmt.Print("\nDigite o número da conta para visualizar logs (ou 0 para voltar): ")
	scanner.Scan()
	var index int
	if _, err := fmt.Sscanf(scanner.Text(), "%d", &index); err != nil {
		fmt.Println("Número inválido!")
		return
	}

	if index == 0 {
		return
	}

	if index < 1 || index > len(accounts) {
		fmt.Println("Número inválido!")
		return
	}

	account := accounts[index-1]
	
	clearScreen()
	fmt.Println("\n=== Opções de Visualização ===")
	fmt.Println("1. Ver últimas 1000 linhas")
	fmt.Println("2. Tail (seguir logs em tempo real)")
	fmt.Println("0. Voltar ao menu anterior")
	fmt.Print("Escolha uma opção: ")
	scanner.Scan()
	viewChoice := strings.TrimSpace(scanner.Text())

	if viewChoice == "0" {
		return
	}

	switch viewChoice {
	case "1":
		viewLogFile(account.ID, account.Name, scanner)
	case "2":
		viewLogTail(account.ID, account.Name, scanner)
	default:
	}
}

func viewLogFile(accountID int64, accountName string, scanner *bufio.Scanner) {
	clearScreen()
	lines, err := readLogFile(accountID, 1000)
	if err != nil {
		fmt.Printf("Erro ao ler arquivo de log: %v\n", err)
		fmt.Println("\nPressione Enter para voltar ao menu anterior...")
		scanner.Scan()
		return
	}

	if len(lines) == 0 {
		fmt.Printf("\nNenhum log encontrado para a conta '%s'.\n", accountName)
		fmt.Println("\nPressione Enter para voltar ao menu anterior...")
		scanner.Scan()
		return
	}

	fmt.Printf("\n=== Logs da conta '%s' (últimas %d linhas) ===\n\n", accountName, len(lines))
	for _, line := range lines {
		fmt.Println(line)
	}
	fmt.Println("\n=== Fim dos logs ===")
	fmt.Println("\nPressione Enter para voltar ao menu anterior...")
	scanner.Scan()
}

func viewLogTail(accountID int64, accountName string, scanner *bufio.Scanner) {
	clearScreen()
	fmt.Printf("\n=== Tail dos logs da conta '%s' ===\n", accountName)
	fmt.Println("\n═══════════════════════════════════════════════════════════")
	fmt.Println("  Pressione ENTER para parar e voltar ao menu principal")
	fmt.Println("═══════════════════════════════════════════════════════════\n")

	stopChan := make(chan struct{})
	lineChan := make(chan string, 100)

	// Goroutine para ler input do usuário
	go func() {
		scanner.Scan()
		close(stopChan)
	}()

	// Goroutine para fazer tail do arquivo
	go func() {
		err := tailLogFile(accountID, stopChan, func(line string) {
			select {
			case lineChan <- line:
			case <-stopChan:
				return
			}
		})
		if err != nil {
			fmt.Printf("Erro ao fazer tail do log: %v\n", err)
		}
		close(lineChan)
	}()

	// Ler e exibir linhas
	for {
		select {
		case <-stopChan:
			fmt.Println("\n=== Parando visualização de logs ===\n")
			return
		case line, ok := <-lineChan:
			if !ok {
				return
			}
			fmt.Println(line)
		}
	}
}

