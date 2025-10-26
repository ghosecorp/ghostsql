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

// Table represents a database table with binary storage
type Table struct {
	Name     string
	Columns  []Column
	Rows     []Row // In-memory cache
	Pages    []*SlottedPage
	PageMgr  *PageManager
	Metadata *metadata.Metadata
	mu       sync.RWMutex
}

// NewTable creates a new table
func NewTable(name string, columns []Column, meta *metadata.Metadata) *Table {
	return &Table{
		Name:     name,
		Columns:  columns,
		Rows:     make([]Row, 0),
		Pages:    make([]*SlottedPage, 0),
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

	// Encode row to binary
	rowData, err := EncodeRow(t.Columns, row)
	if err != nil {
		return fmt.Errorf("failed to encode row: %w", err)
	}

	// Find or create a page with space
	var targetPage *SlottedPage
	for _, page := range t.Pages {
		if !page.IsFull(uint16(len(rowData))) {
			targetPage = page
			break
		}
	}

	if targetPage == nil {
		// Create new page
		pageID := uint64(len(t.Pages))
		targetPage = NewSlottedPage(pageID)
		t.Pages = append(t.Pages, targetPage)
	}

	// Insert into page
	if _, err := targetPage.InsertRow(rowData); err != nil {
		return fmt.Errorf("failed to insert into page: %w", err)
	}

	// Also keep in memory for now
	t.Rows = append(t.Rows, row)
	return nil
}

// Select retrieves rows matching criteria
func (t *Table) Select(columnNames []string, where *WhereClause) ([]Row, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// For now, scan from in-memory rows
	// TODO: Scan from pages directly for true binary storage
	results := make([]Row, 0)

	for _, row := range t.Rows {
		// Apply WHERE filter
		if where != nil {
			if !evaluateWhere(row, where) {
				continue
			}
		}

		if len(columnNames) == 1 && columnNames[0] == "*" {
			// Return all columns
			results = append(results, row)
		} else {
			// Project specific columns
			projected := make(Row)
			for _, colName := range columnNames {
				if val, exists := row[colName]; exists {
					projected[colName] = val
				}
			}
			results = append(results, projected)
		}
	}

	return results, nil
}

// WhereClause represents a simple WHERE condition
type WhereClause struct {
	Column   string
	Operator string
	Value    interface{}
}

// evaluateWhere evaluates a WHERE clause against a row
func evaluateWhere(row Row, where *WhereClause) bool {
	val, exists := row[where.Column]
	if !exists {
		return false
	}

	switch where.Operator {
	case "=":
		return compare(val, where.Value) == 0
	case "!=", "<>":
		return compare(val, where.Value) != 0
	case "<":
		return compare(val, where.Value) < 0
	case "<=":
		return compare(val, where.Value) <= 0
	case ">":
		return compare(val, where.Value) > 0
	case ">=":
		return compare(val, where.Value) >= 0
	default:
		return false
	}
}

// compare compares two values
func compare(a, b interface{}) int {
	// Convert to comparable types
	aInt, aIsInt := toComparableInt(a)
	bInt, bIsInt := toComparableInt(b)

	if aIsInt && bIsInt {
		if aInt < bInt {
			return -1
		} else if aInt > bInt {
			return 1
		}
		return 0
	}

	// String comparison
	aStr := fmt.Sprintf("%v", a)
	bStr := fmt.Sprintf("%v", b)

	if aStr < bStr {
		return -1
	} else if aStr > bStr {
		return 1
	}
	return 0
}

func toComparableInt(val interface{}) (int64, bool) {
	switch v := val.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}

// GetColumnNames returns all column names
func (t *Table) GetColumnNames() []string {
	names := make([]string, len(t.Columns))
	for i, col := range t.Columns {
		names[i] = col.Name
	}
	return names
}

// LoadFromPages reconstructs the table from binary pages
func (t *Table) LoadFromPages() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.Rows = make([]Row, 0)

	for _, page := range t.Pages {
		rowsData := page.GetAllRows()
		for _, rowData := range rowsData {
			row, err := DecodeRow(t.Columns, rowData)
			if err != nil {
				return fmt.Errorf("failed to decode row: %w", err)
			}
			t.Rows = append(t.Rows, row)
		}
	}

	return nil
}
