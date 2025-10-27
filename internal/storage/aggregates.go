// internal/storage/aggregates.go
package storage

import (
	"fmt"
)

// AggregateResult stores the result of an aggregate function
type AggregateResult struct {
	Function string
	Column   string
	Value    interface{}
	Alias    string
}

// ComputeAggregates computes aggregate functions on rows
func ComputeAggregates(rows []Row, aggregates []AggregateSpec) ([]AggregateResult, error) {
	results := make([]AggregateResult, len(aggregates))

	for i, agg := range aggregates {
		var value interface{}
		var err error

		switch agg.Function {
		case "COUNT":
			value = computeCount(rows, agg.Column)
		case "SUM":
			value, err = computeSum(rows, agg.Column)
		case "AVG":
			value, err = computeAvg(rows, agg.Column)
		case "MAX":
			value, err = computeMax(rows, agg.Column)
		case "MIN":
			value, err = computeMin(rows, agg.Column)
		default:
			return nil, fmt.Errorf("unsupported aggregate function: %s", agg.Function)
		}

		if err != nil {
			return nil, err
		}

		results[i] = AggregateResult{
			Function: agg.Function,
			Column:   agg.Column,
			Value:    value,
			Alias:    agg.Alias,
		}
	}

	return results, nil
}

// AggregateSpec specifies an aggregate function
type AggregateSpec struct {
	Function string
	Column   string
	Alias    string
}

func computeCount(rows []Row, column string) int {
	if column == "*" {
		return len(rows)
	}

	count := 0
	for _, row := range rows {
		if val, exists := row[column]; exists && val != nil {
			count++
		}
	}
	return count
}

func computeSum(rows []Row, column string) (float64, error) {
	if column == "*" {
		return 0, fmt.Errorf("SUM(*) is not supported")
	}

	sum := 0.0
	for _, row := range rows {
		val, exists := row[column]
		if !exists || val == nil {
			continue
		}

		numVal, err := convertToFloat64(val)
		if err != nil {
			return 0, fmt.Errorf("SUM requires numeric values")
		}
		sum += numVal
	}
	return sum, nil
}

func computeAvg(rows []Row, column string) (float64, error) {
	if column == "*" {
		return 0, fmt.Errorf("AVG(*) is not supported")
	}

	sum := 0.0
	count := 0

	for _, row := range rows {
		val, exists := row[column]
		if !exists || val == nil {
			continue
		}

		numVal, err := convertToFloat64(val)
		if err != nil {
			return 0, fmt.Errorf("AVG requires numeric values")
		}
		sum += numVal
		count++
	}

	if count == 0 {
		return 0, nil
	}

	return sum / float64(count), nil
}

func computeMax(rows []Row, column string) (interface{}, error) {
	if column == "*" {
		return nil, fmt.Errorf("MAX(*) is not supported")
	}

	if len(rows) == 0 {
		return nil, nil
	}

	var max interface{}
	maxSet := false

	for _, row := range rows {
		val, exists := row[column]
		if !exists || val == nil {
			continue
		}

		if !maxSet {
			max = val
			maxSet = true
			continue
		}

		if compare(val, max) > 0 {
			max = val
		}
	}

	return max, nil
}

func computeMin(rows []Row, column string) (interface{}, error) {
	if column == "*" {
		return nil, fmt.Errorf("MIN(*) is not supported")
	}

	if len(rows) == 0 {
		return nil, nil
	}

	var min interface{}
	minSet := false

	for _, row := range rows {
		val, exists := row[column]
		if !exists || val == nil {
			continue
		}

		if !minSet {
			min = val
			minSet = true
			continue
		}

		if compare(val, min) < 0 {
			min = val
		}
	}

	return min, nil
}

// convertToFloat64 converts interface{} to float64 for aggregates
func convertToFloat64(val interface{}) (float64, error) {
	switch v := val.(type) {
	case int:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert to float64")
	}
}

// GroupByResult represents grouped rows with their aggregate results
type GroupByResult struct {
	GroupKey   map[string]interface{}
	Rows       []Row
	Aggregates []AggregateResult
}

// GroupRows groups rows by specified columns
func GroupRows(rows []Row, groupByColumns []string) map[string]*GroupByResult {
	groups := make(map[string]*GroupByResult)

	for _, row := range rows {
		// Create group key
		key := makeGroupKey(row, groupByColumns)

		if _, exists := groups[key]; !exists {
			groupKey := make(map[string]interface{})
			for _, col := range groupByColumns {
				groupKey[col] = row[col]
			}
			groups[key] = &GroupByResult{
				GroupKey: groupKey,
				Rows:     make([]Row, 0),
			}
		}

		groups[key].Rows = append(groups[key].Rows, row)
	}

	return groups
}

func makeGroupKey(row Row, columns []string) string {
	key := ""
	for i, col := range columns {
		if i > 0 {
			key += "|"
		}
		val := row[col]
		key += fmt.Sprintf("%v", val)
	}
	return key
}
