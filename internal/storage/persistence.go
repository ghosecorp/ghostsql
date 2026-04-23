// internal/storage/persistence.go
package storage

import (
	"fmt"
)

// SaveTableToDisk persists a table to disk in BINARY format
func (db *Database) SaveTableToDisk(dbInstance *DatabaseInstance, table *Table) error {
	return db.saveTableForDatabase(dbInstance, table)
}

// LoadTableFromDisk loads a table from disk
func (db *Database) LoadTableFromDisk(dbInstance *DatabaseInstance, tableName string) (*Table, error) {
	return db.loadTableForDatabase(dbInstance, tableName)
}

// SaveAllTables persists all tables to disk
func (db *Database) SaveAllTables() error {
	for _, dbInstance := range db.Databases {
		for _, table := range dbInstance.Tables {
			if err := db.saveTableForDatabase(dbInstance, table); err != nil {
				return fmt.Errorf("failed to save table %s: %w", table.Name, err)
			}
		}
	}
	return nil
}
