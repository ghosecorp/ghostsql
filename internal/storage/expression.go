package storage

import (
	"strconv"
	"strings"
)

// EvaluateExpression evaluates a simple arithmetic expression on a row
func EvaluateExpression(expr string, row Row) interface{} {
	// Very simple evaluator for expressions like "age + 1" or "salary * 1.1"
	// Support basic operators: +, -, *, /

	// 1. Try to find if it's just a column
	if val, exists := row[expr]; exists {
		return val
	}

	// Try fuzzy matching for joined columns
	for k, v := range row {
		if strings.HasSuffix(k, "."+expr) {
			return v
		}
	}

	// 2. Try to handle simple arithmetic "A op B"
	operators := []string{"+", "-", "*", "/"}
	for _, op := range operators {
		if strings.Contains(expr, op) {
			parts := strings.SplitN(expr, op, 2)
			left := strings.TrimSpace(parts[0])
			right := strings.TrimSpace(parts[1])

			lVal := EvaluateExpression(left, row)
			rVal := EvaluateExpression(right, row)

			return ApplyOperator(lVal, op, rVal)
		}
	}

	// 3. Try to parse as a number
	if f, err := strconv.ParseFloat(expr, 64); err == nil {
		return f
	}

	return nil
}

// ApplyOperator applies an arithmetic operator to two values
func ApplyOperator(left interface{}, op string, right interface{}) interface{} {
	l, errL := ConvertToFloat64(left)
	r, errR := ConvertToFloat64(right)
	if errL != nil || errR != nil {
		return nil
	}

	switch op {
	case "+":
		return l + r
	case "-":
		return l - r
	case "*":
		return l * r
	case "/":
		if r == 0 {
			return nil
		}
		return l / r
	}
	return nil
}
