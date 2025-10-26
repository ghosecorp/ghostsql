package storage

import (
	"fmt"
	"sync"

	"github.com/ghosecorp/ghostsql/internal/metadata"
)

// Column represents a table column definition
type Column struct {
	Name     string
	Type     DataType
	Nullable bool
	Metadata *metadata.Metadata
}

// Row represents a single row of data
type Row map[string]interface{}

// Table represents a database table
type Table struct {
	Name     string
	Columns  []Column
	Rows     []Row
	Metadata *metadata.Metadata
	mu       sync.RWMutex
}

// NewTable creates a new table
func NewTable(name string, columns []Column, meta *metadata.Metadata) *Table {
	return &Table{
		Name:     name,
		Columns:  columns,
		Rows:     make([]Row, 0),
		Metadata: meta,
	}
}

// Insert adds a new row to the table
func (t *Table) Insert(row Row) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Validate row has all required columns
	for _, col := range t.Columns {
		if _, exists := row[col.Name]; !exists && !col.Nullable {
			return fmt.Errorf("missing required column: %s", col.Name)
		}
	}

	t.Rows = append(t.Rows, row)
	return nil
}

// Select retrieves rows matching criteria (simplified - returns all for now)
func (t *Table) Select(columnNames []string) ([]Row, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(columnNames) == 1 && columnNames[0] == "*" {
		// Return all columns
		return t.Rows, nil
	}

	// Project specific columns
	result := make([]Row, len(t.Rows))
	for i, row := range t.Rows {
		projected := make(Row)
		for _, colName := range columnNames {
			if val, exists := row[colName]; exists {
				projected[colName] = val
			}
		}
		result[i] = projected
	}

	return result, nil
}

// GetColumnNames returns all column names
func (t *Table) GetColumnNames() []string {
	names := make([]string, len(t.Columns))
	for i, col := range t.Columns {
		names[i] = col.Name
	}
	return names
}
