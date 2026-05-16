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

	upperExpr := strings.ToUpper(strings.TrimSpace(expr))
	if strings.HasPrefix(upperExpr, "CASE WHEN") {
		return evaluateCaseWhen(expr, row)
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

	// 4. Try to parse as a string literal
	if strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'") && len(expr) >= 2 {
		return expr[1 : len(expr)-1]
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

// evaluateCaseWhen handles CASE WHEN cond THEN val ELSE val END
func evaluateCaseWhen(expr string, row Row) interface{} {
	expr = strings.TrimSpace(expr)
	upperExpr := strings.ToUpper(expr)

	if !strings.HasPrefix(upperExpr, "CASE ") || !strings.HasSuffix(upperExpr, "END") {
		return nil
	}
	inner := strings.TrimSpace(expr[4 : len(expr)-3])

	for {
		inner = strings.TrimSpace(inner)
		if len(inner) == 0 {
			break
		}

		upperInner := strings.ToUpper(inner)
		if strings.HasPrefix(upperInner, "WHEN ") {
			thenIdx := strings.Index(upperInner, " THEN ")
			if thenIdx == -1 {
				break
			}
			condStr := strings.TrimSpace(inner[5:thenIdx])

			nextWhenIdx := strings.Index(upperInner[thenIdx+6:], " WHEN ")
			nextElseIdx := strings.Index(upperInner[thenIdx+6:], " ELSE ")

			endIdx := len(inner)
			if nextWhenIdx != -1 && (nextElseIdx == -1 || nextWhenIdx < nextElseIdx) {
				endIdx = thenIdx + 6 + nextWhenIdx
			} else if nextElseIdx != -1 {
				endIdx = thenIdx + 6 + nextElseIdx
			}

			resStr := strings.TrimSpace(inner[thenIdx+6 : endIdx])
			condEval := EvaluateCondition(condStr, row)

			if condEval {
				return EvaluateExpression(resStr, row)
			}

			inner = inner[endIdx:]
		} else if strings.HasPrefix(upperInner, "ELSE ") {
			resStr := strings.TrimSpace(inner[5:])
			return EvaluateExpression(resStr, row)
		} else {
			break
		}
	}
	return nil
}

// EvaluateCondition evaluates a boolean condition
func EvaluateCondition(cond string, row Row) bool {
	operators := []string{"!=", "<=", ">=", "=", "<", ">"}
	for _, op := range operators {
		if idx := strings.Index(cond, op); idx != -1 {
			left := strings.TrimSpace(cond[:idx])
			right := strings.TrimSpace(cond[idx+len(op):])

			lVal := EvaluateExpression(left, row)
			rVal := EvaluateExpression(right, row)

			res := compare(lVal, rVal)
			switch op {
			case "=":
				return res == 0
			case "!=":
				return res != 0
			case "<":
				return res < 0
			case "<=":
				return res <= 0
			case ">":
				return res > 0
			case ">=":
				return res >= 0
			}
		}
	}

	val := EvaluateExpression(cond, row)
	if b, ok := val.(bool); ok {
		return b
	}
	return false
}
