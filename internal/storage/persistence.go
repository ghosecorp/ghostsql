// internal/storage/persistence.go
package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// SaveTableToDisk persists a table to disk in BINARY format
func (db *Database) SaveTableToDisk(table *Table) error {
	// Use binary format instead of JSON
	return db.SaveTableBinary(table)
}

// LoadTableFromDisk loads a table from disk
func (db *Database) LoadTableFromDisk(tableName string) (*Table, error) {
	// Try binary format first
	return db.LoadTableBinary(tableName)
}

// LoadAllTables loads all tables from disk
func (db *Database) LoadAllTables() error {
	tablesDir := filepath.Join(db.DataDir.DatabasesPath, "default", "tables")

	// Create directory if it doesn't exist
	if err := os.MkdirAll(tablesDir, 0755); err != nil {
		return fmt.Errorf("failed to create tables directory: %w", err)
	}

	// Read directory
	entries, err := os.ReadDir(tablesDir)
	if err != nil {
		return fmt.Errorf("failed to read tables directory: %w", err)
	}

	// Load each .tbl file (binary format)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if strings.HasSuffix(name, ".tbl") {
			tableName := strings.TrimSuffix(name, ".tbl")

			table, err := db.LoadTableFromDisk(tableName)
			if err != nil {
				db.Logger.Error("Failed to load table %s: %v", tableName, err)
				continue
			}

			db.Tables[tableName] = table
		}
	}

	// Clean up old JSON files if they exist
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".json") || strings.HasSuffix(name, ".schema") {
			oldFile := filepath.Join(tablesDir, name)
			os.Remove(oldFile)
			db.Logger.Info("Removed old JSON file: %s", name)
		}
	}

	return nil
}

// SaveAllTables persists all tables to disk
func (db *Database) SaveAllTables() error {
	for _, table := range db.Tables {
		if err := db.SaveTableToDisk(table); err != nil {
			return fmt.Errorf("failed to save table %s: %w", table.Name, err)
		}
	}
	return nil
}
