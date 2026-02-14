package main

import (
	"database/sql"
	"os"
	"path/filepath"
	_ "github.com/mattn/go-sqlite3"
)

type Database struct {
	db *sql.DB
}

func NewDatabase() (*Database, error) {
	// Usar caminho no diretório data para compatibilidade com Docker
	dbPath := "./bybit_accounts.db"
	if dataDir := getDataDir(); dataDir != "" {
		dbPath = filepath.Join(dataDir, "bybit_accounts.db")
	}
	
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	database := &Database{db: db}
	if err := database.initSchema(); err != nil {
		return nil, err
	}

	return database, nil
}

func (d *Database) Close() error {
	return d.db.Close()
}

func (d *Database) initSchema() error {
	// Tabela de contas Bybit
	createAccountsTable := `
	CREATE TABLE IF NOT EXISTS bybit_accounts (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		api_key TEXT NOT NULL,
		api_secret TEXT NOT NULL,
		webhook_url TEXT,
		active INTEGER DEFAULT 1,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`

	// Tabela de conexões ativas
	createConnectionsTable := `
	CREATE TABLE IF NOT EXISTS active_connections (
		account_id INTEGER PRIMARY KEY,
		connected INTEGER DEFAULT 1,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		FOREIGN KEY (account_id) REFERENCES bybit_accounts(id) ON DELETE CASCADE
	);`

	if _, err := d.db.Exec(createAccountsTable); err != nil {
		return err
	}

	if _, err := d.db.Exec(createConnectionsTable); err != nil {
		return err
	}

	// Adicionar novas colunas se não existirem
	if err := d.addColumnIfNotExists("bybit_accounts", "mark_everyone_order", "INTEGER DEFAULT 0"); err != nil {
		return err
	}

	if err := d.addColumnIfNotExists("bybit_accounts", "mark_everyone_wallet", "INTEGER DEFAULT 0"); err != nil {
		return err
	}

	return nil
}

func (d *Database) GetDB() *sql.DB {
	return d.db
}

// addColumnIfNotExists verifica se uma coluna existe na tabela e a adiciona se não existir
func (d *Database) addColumnIfNotExists(tableName, columnName, columnDefinition string) error {
	// Verificar se a coluna já existe usando PRAGMA table_info
	rows, err := d.db.Query("PRAGMA table_info(" + tableName + ")")
	if err != nil {
		return err
	}
	defer rows.Close()

	columnExists := false
	for rows.Next() {
		var cid int
		var name string
		var dataType string
		var notNull int
		var defaultValue interface{}
		var pk int

		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &pk); err != nil {
			return err
		}

		if name == columnName {
			columnExists = true
			break
		}
	}

	if !columnExists {
		alterTableSQL := "ALTER TABLE " + tableName + " ADD COLUMN " + columnName + " " + columnDefinition
		if _, err := d.db.Exec(alterTableSQL); err != nil {
			return err
		}
	}

	return nil
}

func getDataDir() string {
	// Verificar se existe variável de ambiente
	if dataDir := os.Getenv("DATA_DIR"); dataDir != "" {
		return dataDir
	}
	
	// Verificar se existe diretório ./data
	if _, err := os.Stat("./data"); err == nil {
		return "./data"
	}
	
	// Criar diretório data se não existir
	if err := os.MkdirAll("./data", 0755); err == nil {
		return "./data"
	}
	
	return ""
}

