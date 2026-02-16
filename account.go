package main

import (
	"database/sql"
	"errors"
)

type BybitAccount struct {
	ID                 int64
	Name               string
	APIKey             string
	APISecret          string
	WebhookURL         string
	Active             bool
	MarkEveryoneOrder  bool
	MarkEveryoneWallet bool
}

type AccountManager struct {
	db *Database
}

func NewAccountManager(db *Database) *AccountManager {
	return &AccountManager{db: db}
}

func (am *AccountManager) AddAccount(account *BybitAccount) error {
	query := `INSERT INTO bybit_accounts (name, api_key, api_secret, webhook_url, active, mark_everyone_order, mark_everyone_wallet) 
	          VALUES (?, ?, ?, ?, ?, ?, ?)`
	
	markEveryoneOrder := 0
	if account.MarkEveryoneOrder {
		markEveryoneOrder = 1
	}
	markEveryoneWallet := 0
	if account.MarkEveryoneWallet {
		markEveryoneWallet = 1
	}
	active := 0
	if account.Active {
		active = 1
	}
	
	_, err := am.db.GetDB().Exec(query, account.Name, account.APIKey, account.APISecret, 
		account.WebhookURL, active, markEveryoneOrder, markEveryoneWallet)
	return err
}

func (am *AccountManager) RemoveAccount(id int64) error {
	// Remove também a conexão ativa se existir
	_, err := am.db.GetDB().Exec("DELETE FROM active_connections WHERE account_id = ?", id)
	if err != nil {
		return err
	}

	_, err = am.db.GetDB().Exec("DELETE FROM bybit_accounts WHERE id = ?", id)
	return err
}

func (am *AccountManager) ListAccounts() ([]*BybitAccount, error) {
	query := `SELECT id, name, api_key, api_secret, webhook_url, active, mark_everyone_order, mark_everyone_wallet 
	          FROM bybit_accounts ORDER BY id`
	
	rows, err := am.db.GetDB().Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []*BybitAccount
	for rows.Next() {
		acc := &BybitAccount{}
		var active, markEveryoneOrder, markEveryoneWallet int
		err := rows.Scan(&acc.ID, &acc.Name, &acc.APIKey, &acc.APISecret, 
			&acc.WebhookURL, &active, &markEveryoneOrder, &markEveryoneWallet)
		if err != nil {
			return nil, err
		}
		acc.Active = active == 1
		acc.MarkEveryoneOrder = markEveryoneOrder == 1
		acc.MarkEveryoneWallet = markEveryoneWallet == 1
		accounts = append(accounts, acc)
	}

	return accounts, rows.Err()
}

func (am *AccountManager) GetAccount(id int64) (*BybitAccount, error) {
	query := `SELECT id, name, api_key, api_secret, webhook_url, active, mark_everyone_order, mark_everyone_wallet 
	          FROM bybit_accounts WHERE id = ?`
	
	acc := &BybitAccount{}
	var active, markEveryoneOrder, markEveryoneWallet int
	err := am.db.GetDB().QueryRow(query, id).Scan(
		&acc.ID, &acc.Name, &acc.APIKey, &acc.APISecret, 
		&acc.WebhookURL, &active, &markEveryoneOrder, &markEveryoneWallet)
	
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.New("conta não encontrada")
		}
		return nil, err
	}
	
	acc.Active = active == 1
	acc.MarkEveryoneOrder = markEveryoneOrder == 1
	acc.MarkEveryoneWallet = markEveryoneWallet == 1
	return acc, nil
}

func (am *AccountManager) SetConnectionActive(accountID int64, active bool) error {
	if active {
		query := `INSERT OR REPLACE INTO active_connections (account_id, connected, updated_at) 
		          VALUES (?, 1, CURRENT_TIMESTAMP)`
		_, err := am.db.GetDB().Exec(query, accountID)
		return err
	} else {
		_, err := am.db.GetDB().Exec("DELETE FROM active_connections WHERE account_id = ?", accountID)
		return err
	}
}

func (am *AccountManager) GetActiveConnections() ([]int64, error) {
	query := `SELECT account_id FROM active_connections WHERE connected = 1`
	rows, err := am.db.GetDB().Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accountIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		accountIDs = append(accountIDs, id)
	}

	return accountIDs, rows.Err()
}

func (am *AccountManager) UpdateAccount(id int64, name, webhookURL string, markEveryoneOrder, markEveryoneWallet bool) error {
	markEveryoneOrderInt := 0
	if markEveryoneOrder {
		markEveryoneOrderInt = 1
	}
	markEveryoneWalletInt := 0
	if markEveryoneWallet {
		markEveryoneWalletInt = 1
	}
	
	query := `UPDATE bybit_accounts SET name = ?, webhook_url = ?, mark_everyone_order = ?, mark_everyone_wallet = ? WHERE id = ?`
	_, err := am.db.GetDB().Exec(query, name, webhookURL, markEveryoneOrderInt, markEveryoneWalletInt, id)
	return err
}

// Métodos para gerenciar ordens
func (am *AccountManager) SaveOrder(orderID string, accountID int64, orderDataJSON string) error {
	query := `INSERT OR REPLACE INTO orders (order_id, account_id, order_data) VALUES (?, ?, ?)`
	_, err := am.db.GetDB().Exec(query, orderID, accountID, orderDataJSON)
	return err
}

func (am *AccountManager) GetOrder(orderID string) (string, error) {
	var orderData string
	query := `SELECT order_data FROM orders WHERE order_id = ?`
	err := am.db.GetDB().QueryRow(query, orderID).Scan(&orderData)
	if err != nil {
		return "", err
	}
	return orderData, nil
}

func (am *AccountManager) DeleteOrder(orderID string) error {
	query := `DELETE FROM orders WHERE order_id = ?`
	_, err := am.db.GetDB().Exec(query, orderID)
	return err
}

