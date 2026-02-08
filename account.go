package main

import (
	"database/sql"
	"errors"
)

type BybitAccount struct {
	ID         int64
	Name       string
	APIKey     string
	APISecret  string
	WebhookURL string
	Active     bool
}

type AccountManager struct {
	db *Database
}

func NewAccountManager(db *Database) *AccountManager {
	return &AccountManager{db: db}
}

func (am *AccountManager) AddAccount(account *BybitAccount) error {
	query := `INSERT INTO bybit_accounts (name, api_key, api_secret, webhook_url, active) 
	          VALUES (?, ?, ?, ?, ?)`
	
	_, err := am.db.GetDB().Exec(query, account.Name, account.APIKey, account.APISecret, 
		account.WebhookURL, account.Active)
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
	query := `SELECT id, name, api_key, api_secret, webhook_url, active 
	          FROM bybit_accounts ORDER BY id`
	
	rows, err := am.db.GetDB().Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []*BybitAccount
	for rows.Next() {
		acc := &BybitAccount{}
		var active int
		err := rows.Scan(&acc.ID, &acc.Name, &acc.APIKey, &acc.APISecret, 
			&acc.WebhookURL, &active)
		if err != nil {
			return nil, err
		}
		acc.Active = active == 1
		accounts = append(accounts, acc)
	}

	return accounts, rows.Err()
}

func (am *AccountManager) GetAccount(id int64) (*BybitAccount, error) {
	query := `SELECT id, name, api_key, api_secret, webhook_url, active 
	          FROM bybit_accounts WHERE id = ?`
	
	acc := &BybitAccount{}
	var active int
	err := am.db.GetDB().QueryRow(query, id).Scan(
		&acc.ID, &acc.Name, &acc.APIKey, &acc.APISecret, 
		&acc.WebhookURL, &active)
	
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.New("conta não encontrada")
		}
		return nil, err
	}
	
	acc.Active = active == 1
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

func (am *AccountManager) UpdateAccount(id int64, name, webhookURL string) error {
	query := `UPDATE bybit_accounts SET name = ?, webhook_url = ? WHERE id = ?`
	_, err := am.db.GetDB().Exec(query, name, webhookURL, id)
	return err
}

