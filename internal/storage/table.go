package storage

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/ghosecorp/ghostsql/internal/metadata"
)

// Column represents a table column definition
type Column struct {
	Name        string
	Type        DataType
	Length      int
	Nullable    bool
	IsPrimary   bool
	IsUnique    bool
	DefaultVal  interface{}
	DefaultExpr string
	CheckExpr   string
	ForeignKey  *ForeignKeyConstraint // Add this
	Metadata    *metadata.Metadata
}

// Row represents a single row of data
type Row map[string]interface{}

// Policy represents a Row-Level Security policy
type Policy struct {
	Name       string
	Action     string // "SELECT", "INSERT", "UPDATE", "DELETE"
	Role       string // "all", or specific role name
	UsingExpr  string // SQL expression
	Where      *WhereClause
}

// Table represents a database table with binary storage
type Table struct {
	Name          string
	Owner         string                // PostgreSQL-standard: creator/owner of the table
	Columns       []Column
	Rows          []Row // In-memory cache
	Pages         []*SlottedPage
	PageMgr       *PageManager
	Metadata      *metadata.Metadata
	VectorIndexes map[string]*HNSWIndex // column_name -> index
	RLSEnabled    bool                  // Row-Level Security enabled
	Policies      []Policy              // RLS policies
	mu            sync.RWMutex
}

// ForeignKeyConstraint represents a foreign key relationship
type ForeignKeyConstraint struct {
	RefTable  string
	RefColumn string
}

// NewTable creates a new table
func NewTable(name, owner string, columns []Column, meta *metadata.Metadata) *Table {
	return &Table{
		Name:          name,
		Owner:         owner,
		Columns:       columns,
		Rows:          make([]Row, 0),
		Pages:         make([]*SlottedPage, 0),
		Metadata:      meta,
		VectorIndexes: make(map[string]*HNSWIndex),
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
	And      *WhereClause
	Or       *WhereClause
}

func (w *WhereClause) Clone() *WhereClause {
	if w == nil {
		return nil
	}
	newW := &WhereClause{
		Column:   w.Column,
		Operator: w.Operator,
		Value:    w.Value,
	}
	if w.And != nil {
		newW.And = w.And.Clone()
	}
	if w.Or != nil {
		newW.Or = w.Or.Clone()
	}
	return newW
}

// evaluateWhere evaluates a WHERE clause against a row
func evaluateWhere(row Row, where *WhereClause) bool {
	// Base condition evaluation
	match := false

	// Skip system function calls or empty column containers
	if where.Column == "" || strings.Contains(where.Column, "(") {
		match = true
	} else {
		val, exists := row[where.Column]
		if !exists {
			// Try evaluate as arithmetic expression
			val = EvaluateExpression(where.Column, row)
			if val != nil {
				exists = true
			}
		}

		if !exists {
			// Try fuzzy matching for joined columns
			for k, v := range row {
				if strings.HasSuffix(k, "."+where.Column) || strings.HasSuffix(where.Column, "."+k) {
					val = v
					exists = true
					break
				}
			}
		}

		if !exists {
			// If column doesn't exist, it's a mismatch unless we're looking for NULL
			match = (where.Operator == "=" && where.Value == nil)
		} else {
			switch where.Operator {
			case "=":
				match = compare(val, where.Value) == 0
			case "!=", "<>":
				match = compare(val, where.Value) != 0
			case "<":
				match = compare(val, where.Value) < 0
			case "<=":
				match = compare(val, where.Value) <= 0
			case ">":
				match = compare(val, where.Value) > 0
			case ">=":
				match = compare(val, where.Value) >= 0
			case "@>":
				match = EvaluateJsonContain(val, where.Value)
			case "IS NULL":
				match = val == nil
			case "IS NOT NULL":
				match = val != nil
			case "LIKE":
				pattern := strings.ReplaceAll(fmt.Sprintf("%v", where.Value), "%", ".*")
				pattern = strings.ReplaceAll(pattern, "_", ".")
				match, _ = regexp.MatchString("(?i)^"+pattern+"$", fmt.Sprintf("%v", val))
			case "NOT LIKE":
				pattern := strings.ReplaceAll(fmt.Sprintf("%v", where.Value), "%", ".*")
				pattern = strings.ReplaceAll(pattern, "_", ".")
				matched, _ := regexp.MatchString("(?i)^"+pattern+"$", fmt.Sprintf("%v", val))
				match = !matched
			case "ILIKE":
				pattern := strings.ReplaceAll(fmt.Sprintf("%v", where.Value), "%", ".*")
				pattern = strings.ReplaceAll(pattern, "_", ".")
				match, _ = regexp.MatchString("(?i)^"+pattern+"$", fmt.Sprintf("%v", val))
			case "NOT ILIKE":
				pattern := strings.ReplaceAll(fmt.Sprintf("%v", where.Value), "%", ".*")
				pattern = strings.ReplaceAll(pattern, "_", ".")
				matched, _ := regexp.MatchString("(?i)^"+pattern+"$", fmt.Sprintf("%v", val))
				match = !matched
			case "~":
				match, _ = regexp.MatchString(fmt.Sprintf("%v", where.Value), fmt.Sprintf("%v", val))
			case "~*":
				match, _ = regexp.MatchString("(?i)"+fmt.Sprintf("%v", where.Value), fmt.Sprintf("%v", val))
			case "!~":
				matched, _ := regexp.MatchString(fmt.Sprintf("%v", where.Value), fmt.Sprintf("%v", val))
				match = !matched
			case "!~*":
				matched, _ := regexp.MatchString("(?i)"+fmt.Sprintf("%v", where.Value), fmt.Sprintf("%v", val))
				match = !matched
			case "IN":
				if list, ok := where.Value.([]interface{}); ok {
					for _, item := range list {
						if compare(val, item) == 0 {
							match = true
							break
						}
					}
				}
			case "NOT IN":
				match = true
				if list, ok := where.Value.([]interface{}); ok {
					for _, item := range list {
						if compare(val, item) == 0 {
							match = false
							break
						}
					}
				}
			case "BETWEEN":
				if bounds, ok := where.Value.([]interface{}); ok && len(bounds) == 2 {
					match = compare(val, bounds[0]) >= 0 && compare(val, bounds[1]) <= 0
				}
			case "NOT BETWEEN":
				if bounds, ok := where.Value.([]interface{}); ok && len(bounds) == 2 {
					match = compare(val, bounds[0]) < 0 || compare(val, bounds[1]) > 0
				}
			default:
				match = false
			}
		}
	}

	// Handle AND/OR chains
	if where.And != nil {
		return match && evaluateWhere(row, where.And)
	}
	if where.Or != nil {
		return match || evaluateWhere(row, where.Or)
	}

	return match
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

// Update updates rows matching the WHERE clause
func (t *Table) Update(updates map[string]interface{}, where *WhereClause) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	updatedCount := 0

	for i := range t.Rows {
		if where != nil && !evaluateWhere(t.Rows[i], where) {
			continue
		}

		// Update the row
		for colName, newValue := range updates {
			t.Rows[i][colName] = newValue
		}
		updatedCount++
	}

	return updatedCount, nil
}

// Delete deletes rows matching the WHERE clause
func (t *Table) Delete(where *WhereClause) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if where == nil {
		// Delete all rows
		count := len(t.Rows)
		t.Rows = make([]Row, 0)
		return count, nil
	}

	// Filter out rows that match the WHERE clause
	newRows := make([]Row, 0)
	deletedCount := 0

	for _, row := range t.Rows {
		if evaluateWhere(row, where) {
			deletedCount++
		} else {
			newRows = append(newRows, row)
		}
	}

	t.Rows = newRows
	return deletedCount, nil
}

// Truncate removes all rows from the table
func (t *Table) Truncate() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.Rows = make([]Row, 0)
	t.Pages = make([]*SlottedPage, 0)
	return nil
}

// AddColumn adds a new column to the table
func (t *Table) AddColumn(col Column) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if column already exists
	for _, existingCol := range t.Columns {
		if existingCol.Name == col.Name {
			return fmt.Errorf("column %s already exists", col.Name)
		}
	}

	// Add column to schema
	t.Columns = append(t.Columns, col)

	// Add NULL values to existing rows
	for i := range t.Rows {
		t.Rows[i][col.Name] = nil
	}

	return nil
}

// matchLike performs SQL LIKE pattern matching
// Supports % (any characters) and _ (single character)
func matchLike(value, pattern interface{}) bool {
	valStr := fmt.Sprintf("%v", value)
	patStr := fmt.Sprintf("%v", pattern)

	return matchLikePattern(valStr, patStr)
}

// matchLikePattern implements the LIKE pattern matching algorithm
func matchLikePattern(str, pattern string) bool {
	// Convert to lowercase for case-insensitive matching (like PostgreSQL's ILIKE)
	// Remove this for case-sensitive LIKE
	str = strings.ToLower(str)
	pattern = strings.ToLower(pattern)

	return matchLikeRecursive(str, pattern, 0, 0)
}

// matchLikeRecursive recursively matches string against pattern
func matchLikeRecursive(str, pattern string, sIdx, pIdx int) bool {
	// Base cases
	if pIdx == len(pattern) {
		return sIdx == len(str)
	}

	if sIdx == len(str) {
		// Check if remaining pattern is all %
		for i := pIdx; i < len(pattern); i++ {
			if pattern[i] != '%' {
				return false
			}
		}
		return true
	}

	// Handle pattern characters
	if pattern[pIdx] == '%' {
		// Try matching zero or more characters
		// First try skipping the % (matching zero characters)
		if matchLikeRecursive(str, pattern, sIdx, pIdx+1) {
			return true
		}
		// Try matching one more character and continue with %
		return matchLikeRecursive(str, pattern, sIdx+1, pIdx)
	}

	if pattern[pIdx] == '_' {
		// Match exactly one character
		return matchLikeRecursive(str, pattern, sIdx+1, pIdx+1)
	}

	// Regular character match
	if pattern[pIdx] == str[sIdx] {
		return matchLikeRecursive(str, pattern, sIdx+1, pIdx+1)
	}

	return false
}

// DropColumn drops a column from the table
func (t *Table) DropColumn(colName string, ifExists bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	foundIdx := -1
	for i, col := range t.Columns {
		if col.Name == colName {
			foundIdx = i
			break
		}
	}

	if foundIdx == -1 {
		if ifExists {
			return nil
		}
		return fmt.Errorf("column %s does not exist", colName)
	}

	// Remove from Columns
	t.Columns = append(t.Columns[:foundIdx], t.Columns[foundIdx+1:]...)

	// Remove from Rows
	for i := range t.Rows {
		delete(t.Rows[i], colName)
	}

	return nil
}

// RenameColumn renames a column in the table
func (t *Table) RenameColumn(oldName, newName string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	found := false
	for i, col := range t.Columns {
		if col.Name == oldName {
			t.Columns[i].Name = newName
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("column %s does not exist", oldName)
	}

	for i := range t.Rows {
		if val, exists := t.Rows[i][oldName]; exists {
			t.Rows[i][newName] = val
			delete(t.Rows[i], oldName)
		}
	}

	return nil
}

// AlterColumnType alters a column's datatype, converting existing values
func (t *Table) AlterColumnType(colName string, newType DataType) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	found := false
	for i, col := range t.Columns {
		if col.Name == colName {
			t.Columns[i].Type = newType
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("column %s does not exist", colName)
	}

	// Convert all existing values
	for i := range t.Rows {
		val, exists := t.Rows[i][colName]
		if !exists || val == nil {
			continue
		}

		var converted interface{}
		var err error
		switch newType {
		case TypeInt:
			switch v := val.(type) {
			case int:
				converted = v
			case int32:
				converted = int(v)
			case int64:
				converted = int(v)
			case float64:
				converted = int(v)
			case string:
				var iv int
				_, err = fmt.Sscanf(v, "%d", &iv)
				converted = iv
			default:
				err = fmt.Errorf("cannot convert value %v to INT", val)
			}
		case TypeFloat:
			switch v := val.(type) {
			case float64:
				converted = v
			case int:
				converted = float64(v)
			case int32:
				converted = float64(v)
			case int64:
				converted = float64(v)
			case string:
				var fv float64
				_, err = fmt.Sscanf(v, "%f", &fv)
				converted = fv
			default:
				err = fmt.Errorf("cannot convert value %v to FLOAT", val)
			}
		case TypeText, TypeVarChar:
			converted = fmt.Sprintf("%v", val)
		case TypeBoolean:
			switch v := val.(type) {
			case bool:
				converted = v
			case string:
				lower := strings.ToLower(v)
				if lower == "true" || lower == "t" || lower == "1" || lower == "yes" {
					converted = true
				} else if lower == "false" || lower == "f" || lower == "0" || lower == "no" {
					converted = false
				} else {
					err = fmt.Errorf("cannot convert string %s to BOOLEAN", v)
				}
			case int:
				converted = v != 0
			default:
				err = fmt.Errorf("cannot convert value %v to BOOLEAN", val)
			}
		default:
			converted = val
		}

		if err != nil {
			return fmt.Errorf("failed to alter column type for column %s: %w", colName, err)
		}
		t.Rows[i][colName] = converted
	}

	return nil
}

// Clone returns a deep copy of the Table
func (t *Table) Clone() *Table {
	t.mu.RLock()
	defer t.mu.RUnlock()

	clonedCols := make([]Column, len(t.Columns))
	copy(clonedCols, t.Columns)

	clonedRows := make([]Row, len(t.Rows))
	for i, r := range t.Rows {
		clonedRow := make(Row)
		for k, v := range r {
			clonedRow[k] = v
		}
		clonedRows[i] = clonedRow
	}

	clonedPolicies := make([]Policy, len(t.Policies))
	copy(clonedPolicies, t.Policies)

	return &Table{
		Name:          t.Name,
		Owner:         t.Owner,
		Columns:       clonedCols,
		Rows:          clonedRows,
		RLSEnabled:    t.RLSEnabled,
		Policies:      clonedPolicies,
		Metadata:      t.Metadata,
		VectorIndexes: t.VectorIndexes,
	}
}

