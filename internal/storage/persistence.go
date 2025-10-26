package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// TableSchema represents the schema for serialization
type TableSchema struct {
	Name     string
	Columns  []Column
	Metadata map[string]string
}

// SaveTableToDisk persists a table to disk
func (db *Database) SaveTableToDisk(table *Table) error {
	// Create table file path
	tablePath := filepath.Join(db.DataDir.DatabasesPath, "default", "tables", table.Name+".json")

	// Ensure directory exists
	dir := filepath.Dir(tablePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create table directory: %w", err)
	}

	// Prepare schema
	schema := TableSchema{
		Name:    table.Name,
		Columns: table.Columns,
	}
	if table.Metadata != nil {
		schema.Metadata = map[string]string{
			"purpose":     table.Metadata.Purpose,
			"description": table.Metadata.Description,
		}
	}

	// Write schema
	schemaPath := filepath.Join(db.DataDir.DatabasesPath, "default", "tables", table.Name+".schema")
	schemaData, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}
	if err := os.WriteFile(schemaPath, schemaData, 0644); err != nil {
		return fmt.Errorf("failed to write schema: %w", err)
	}

	// Write data
	dataData, err := json.MarshalIndent(table.Rows, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}
	if err := os.WriteFile(tablePath, dataData, 0644); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	db.Logger.Info("Saved table %s to disk", table.Name)
	return nil
}

// LoadTableFromDisk loads a table from disk
func (db *Database) LoadTableFromDisk(tableName string) (*Table, error) {
	schemaPath := filepath.Join(db.DataDir.DatabasesPath, "default", "tables", tableName+".schema")
	dataPath := filepath.Join(db.DataDir.DatabasesPath, "default", "tables", tableName+".json")

	// Read schema
	schemaData, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema: %w", err)
	}

	var schema TableSchema
	if err := json.Unmarshal(schemaData, &schema); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema: %w", err)
	}

	// Create table with schema
	table := &Table{
		Name:    schema.Name,
		Columns: schema.Columns,
		Rows:    make([]Row, 0),
	}

	// Read data
	dataData, err := os.ReadFile(dataPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No data yet, that's okay
			return table, nil
		}
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	if err := json.Unmarshal(dataData, &table.Rows); err != nil {
		return nil, fmt.Errorf("failed to unmarshal data: %w", err)
	}

	db.Logger.Info("Loaded table %s from disk (%d rows)", tableName, len(table.Rows))
	return table, nil
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

	// Load each .schema file
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if filepath.Ext(name) == ".schema" {
			tableName := name[:len(name)-7] // Remove .schema extension

			table, err := db.LoadTableFromDisk(tableName)
			if err != nil {
				db.Logger.Error("Failed to load table %s: %v", tableName, err)
				continue
			}

			db.Tables[tableName] = table
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
