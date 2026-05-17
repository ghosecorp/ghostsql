package storage

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"
)

// EvaluateExpression evaluates a simple arithmetic or function expression on a row
func EvaluateExpression(expr string, row Row) interface{} {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil
	}

	// 1. Try to find if it's just a column key in the row
	if val, exists := row[expr]; exists {
		return val
	}

	// Try fuzzy matching for joined columns (table.col notation)
	for k, v := range row {
		if strings.HasSuffix(k, "."+expr) {
			return v
		}
	}

	upper := strings.ToUpper(expr)

	// JSON text extraction ->>
	if pos := findOperatorOutsideParens(expr, "->>"); pos >= 0 {
		left := strings.TrimSpace(expr[:pos])
		right := strings.TrimSpace(expr[pos+3:])
		if strings.HasPrefix(right, "'") && strings.HasSuffix(right, "'") && len(right) >= 2 {
			right = right[1 : len(right)-1]
		}
		lVal := EvaluateExpression(left, row)
		return evaluateJsonExtract(lVal, right, true)
	}

	// JSON object extraction ->
	if pos := findOperatorOutsideParens(expr, "->"); pos >= 0 {
		left := strings.TrimSpace(expr[:pos])
		right := strings.TrimSpace(expr[pos+2:])
		if strings.HasPrefix(right, "'") && strings.HasSuffix(right, "'") && len(right) >= 2 {
			right = right[1 : len(right)-1]
		}
		lVal := EvaluateExpression(left, row)
		return evaluateJsonExtract(lVal, right, false)
	}

	// JSON containment @>
	if pos := findOperatorOutsideParens(expr, "@>"); pos >= 0 {
		left := strings.TrimSpace(expr[:pos])
		right := strings.TrimSpace(expr[pos+2:])
		if strings.HasPrefix(right, "'") && strings.HasSuffix(right, "'") && len(right) >= 2 {
			right = right[1 : len(right)-1]
		}
		lVal := EvaluateExpression(left, row)
		rVal := EvaluateExpression(right, row)
		return EvaluateJsonContain(lVal, rVal)
	}

	// 2. CASE WHEN ... END
	if strings.HasPrefix(upper, "CASE WHEN") {
		return evaluateCaseWhen(expr, row)
	}

	// 3. CAST(expr AS type) or CAST expression from :: parsing
	if strings.HasPrefix(upper, "CAST(") {
		return evaluateCast(expr, row)
	}

	// 3.5 Type casting using :: syntax
	// Note: We check if it's outside string literals
	if castIdx := findOperatorOutsideParens(expr, "::"); castIdx > 0 {
		baseExpr := expr[:castIdx]
		castType := strings.ToUpper(strings.TrimSpace(expr[castIdx+2:]))
		baseVal := EvaluateExpression(baseExpr, row)
		return applyTypeCast(baseVal, castType)
	}

	// 4. Function calls: LOWER(), UPPER(), ABS(), NOW(), etc.
	if idx := strings.Index(expr, "("); idx > 0 && strings.HasSuffix(strings.TrimSpace(expr), ")") {
		fnName := strings.ToUpper(strings.TrimSpace(expr[:idx]))
		argsStr := expr[idx+1 : len(expr)-1]
		return evaluateFunctionCall(fnName, argsStr, row)
	}

	// 5. Simple arithmetic "A op B" — check for binary operators
	// Must be careful not to split inside parentheses
	for _, op := range []string{"+", "-", "*", "/"} {
		if pos := findOperatorOutsideParens(expr, op); pos >= 0 {
			left := strings.TrimSpace(expr[:pos])
			right := strings.TrimSpace(expr[pos+len(op):])
			lVal := EvaluateExpression(left, row)
			rVal := EvaluateExpression(right, row)
			return ApplyOperator(lVal, op, rVal)
		}
	}

	// 6. Try to parse as a number
	if f, err := strconv.ParseFloat(expr, 64); err == nil {
		return f
	}

	// 7. Try to parse as a string literal (single-quoted)
	if strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'") && len(expr) >= 2 {
		return expr[1 : len(expr)-1]
	}

	// 7.5 Fallback for bare word date parts
	u := strings.ToUpper(expr)
	if u == "YEAR" || u == "MONTH" || u == "DAY" || u == "HOUR" || u == "MINUTE" || u == "SECOND" || u == "EPOCH" || u == "DOW" {
		return expr
	}

	return nil
}

// findOperatorOutsideParens finds the rightmost position of op that's not inside parentheses or string literals
func findOperatorOutsideParens(expr, op string) int {
	depth := 0
	inString := false
	result := -1
	i := 0
	for i < len(expr) {
		ch := expr[i]
		if ch == '\'' && !inString {
			inString = true
			i++
			continue
		}
		if ch == '\'' && inString {
			inString = false
			i++
			continue
		}
		if inString {
			i++
			continue
		}
		if ch == '(' {
			depth++
		} else if ch == ')' {
			depth--
		} else if depth == 0 && strings.HasPrefix(expr[i:], op) {
			result = i
		}
		i++
	}
	return result
}

// evaluateFunctionCall dispatches named functions
func evaluateFunctionCall(fnName, argsStr string, row Row) interface{} {
	// Parse comma-separated arguments, respecting nested parens
	args := splitArgs(argsStr, row)

	switch fnName {
	case "JSONB_PATH_QUERY":
		if len(args) >= 2 {
			return evaluateJsonPath(args[0], toString(args[1]))
		}

	// ---- String functions ----
	case "LOWER":
		if len(args) == 1 {
			return strings.ToLower(toString(args[0]))
		}
	case "UPPER":
		if len(args) == 1 {
			return strings.ToUpper(toString(args[0]))
		}
	case "LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH":
		if len(args) == 1 {
			return len([]rune(toString(args[0])))
		}
	case "TRIM":
		if len(args) == 1 {
			return strings.TrimSpace(toString(args[0]))
		}
	case "LTRIM":
		if len(args) == 1 {
			return strings.TrimLeft(toString(args[0]), " \t")
		}
	case "RTRIM":
		if len(args) == 1 {
			return strings.TrimRight(toString(args[0]), " \t")
		}
	case "CONCAT":
		result := ""
		for _, a := range args {
			result += toString(a)
		}
		return result
	case "SUBSTRING", "SUBSTR":
		if len(args) >= 2 {
			s := toString(args[0])
			start := toInt(args[1]) - 1 // SQL is 1-based
			if start < 0 {
				start = 0
			}
			if len(args) >= 3 {
				length := toInt(args[2])
				end := start + length
				runes := []rune(s)
				if start >= len(runes) {
					return ""
				}
				if end > len(runes) {
					end = len(runes)
				}
				return string(runes[start:end])
			}
			runes := []rune(s)
			if start >= len(runes) {
				return ""
			}
			return string(runes[start:])
		}
	case "REPLACE":
		if len(args) == 3 {
			return strings.ReplaceAll(toString(args[0]), toString(args[1]), toString(args[2]))
		}
	case "LEFT":
		if len(args) == 2 {
			s := toString(args[0])
			n := toInt(args[1])
			runes := []rune(s)
			if n > len(runes) {
				n = len(runes)
			}
			if n < 0 {
				n = 0
			}
			return string(runes[:n])
		}
	case "RIGHT":
		if len(args) == 2 {
			s := toString(args[0])
			n := toInt(args[1])
			runes := []rune(s)
			if n > len(runes) {
				n = len(runes)
			}
			start := len(runes) - n
			if start < 0 {
				start = 0
			}
			return string(runes[start:])
		}
	case "LPAD":
		if len(args) >= 2 {
			s := toString(args[0])
			width := toInt(args[1])
			pad := " "
			if len(args) >= 3 {
				pad = toString(args[2])
			}
			for len(s) < width {
				s = pad + s
			}
			return s
		}
	case "RPAD":
		if len(args) >= 2 {
			s := toString(args[0])
			width := toInt(args[1])
			pad := " "
			if len(args) >= 3 {
				pad = toString(args[2])
			}
			for len(s) < width {
				s = s + pad
			}
			return s
		}
	case "REPEAT":
		if len(args) == 2 {
			return strings.Repeat(toString(args[0]), toInt(args[1]))
		}
	case "REVERSE":
		if len(args) == 1 {
			runes := []rune(toString(args[0]))
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			return string(runes)
		}
	case "SPLIT_PART":
		if len(args) == 3 {
			parts := strings.Split(toString(args[0]), toString(args[1]))
			idx := toInt(args[2]) - 1
			if idx >= 0 && idx < len(parts) {
				return parts[idx]
			}
			return ""
		}
	case "POSITION":
		if len(args) == 2 {
			// POSITION(substr IN str) — simplified: treat as POSITION(substr, str)
			substr := toString(args[0])
			str := toString(args[1])
			idx := strings.Index(str, substr)
			if idx == -1 {
				return 0
			}
			return idx + 1
		}
	case "STRPOS":
		if len(args) == 2 {
			idx := strings.Index(toString(args[0]), toString(args[1]))
			if idx == -1 {
				return 0
			}
			return idx + 1
		}
	case "INITCAP":
		if len(args) == 1 {
			words := strings.Fields(toString(args[0]))
			for i, w := range words {
				if len(w) > 0 {
					words[i] = strings.ToUpper(w[:1]) + strings.ToLower(w[1:])
				}
			}
			return strings.Join(words, " ")
		}
	case "TO_CHAR":
		// Simplified: just convert to string
		if len(args) >= 1 {
			return toString(args[0])
		}
	case "COALESCE":
		for _, a := range args {
			if a != nil {
				return a
			}
		}
		return nil
	case "NULLIF":
		if len(args) == 2 {
			if compare(args[0], args[1]) == 0 {
				return nil
			}
			return args[0]
		}
	case "GREATEST":
		if len(args) > 0 {
			best := args[0]
			for _, a := range args[1:] {
				if compare(a, best) > 0 {
					best = a
				}
			}
			return best
		}
	case "LEAST":
		if len(args) > 0 {
			best := args[0]
			for _, a := range args[1:] {
				if compare(a, best) < 0 {
					best = a
				}
			}
			return best
		}

	// ---- Math functions ----
	case "ABS":
		if len(args) == 1 {
			if f, err := ConvertToFloat64(args[0]); err == nil {
				return math.Abs(f)
			}
		}
	case "CEIL", "CEILING":
		if len(args) == 1 {
			if f, err := ConvertToFloat64(args[0]); err == nil {
				return math.Ceil(f)
			}
		}
	case "FLOOR":
		if len(args) == 1 {
			if f, err := ConvertToFloat64(args[0]); err == nil {
				return math.Floor(f)
			}
		}
	case "ROUND":
		if len(args) >= 1 {
			if f, err := ConvertToFloat64(args[0]); err == nil {
				if len(args) == 2 {
					places := toInt(args[1])
					factor := math.Pow(10, float64(places))
					return math.Round(f*factor) / factor
				}
				return math.Round(f)
			}
		}
	case "POWER", "POW":
		if len(args) == 2 {
			base, err1 := ConvertToFloat64(args[0])
			exp, err2 := ConvertToFloat64(args[1])
			if err1 == nil && err2 == nil {
				return math.Pow(base, exp)
			}
		}
	case "SQRT":
		if len(args) == 1 {
			if f, err := ConvertToFloat64(args[0]); err == nil {
				return math.Sqrt(f)
			}
		}
	case "EXP":
		if len(args) == 1 {
			if f, err := ConvertToFloat64(args[0]); err == nil {
				return math.Exp(f)
			}
		}
	case "LN":
		if len(args) == 1 {
			if f, err := ConvertToFloat64(args[0]); err == nil {
				return math.Log(f)
			}
		}
	case "LOG":
		if len(args) == 1 {
			if f, err := ConvertToFloat64(args[0]); err == nil {
				return math.Log10(f)
			}
		} else if len(args) == 2 {
			base, err1 := ConvertToFloat64(args[0])
			val, err2 := ConvertToFloat64(args[1])
			if err1 == nil && err2 == nil {
				return math.Log(val) / math.Log(base)
			}
		}
	case "MOD":
		if len(args) == 2 {
			a, err1 := ConvertToFloat64(args[0])
			b, err2 := ConvertToFloat64(args[1])
			if err1 == nil && err2 == nil && b != 0 {
				return math.Mod(a, b)
			}
		}
	case "TRUNC", "TRUNCATE":
		if len(args) >= 1 {
			if f, err := ConvertToFloat64(args[0]); err == nil {
				if len(args) == 2 {
					places := toInt(args[1])
					factor := math.Pow(10, float64(places))
					return math.Trunc(f*factor) / factor
				}
				return math.Trunc(f)
			}
		}
	case "SIGN":
		if len(args) == 1 {
			if f, err := ConvertToFloat64(args[0]); err == nil {
				if f > 0 {
					return 1.0
				} else if f < 0 {
					return -1.0
				}
				return 0.0
			}
		}
	case "RANDOM":
		// Simplified — return 0.5 (deterministic for tests)
		return 0.5
	case "PI":
		return math.Pi

	// ---- Date / Time functions ----
	case "NOW", "CURRENT_TIMESTAMP", "CLOCK_TIMESTAMP":
		return time.Now().Format(time.RFC3339)
	case "CURRENT_DATE":
		return time.Now().Format("2006-01-02")
	case "CURRENT_TIME":
		return time.Now().Format("15:04:05")
	case "DATE_TRUNC":
		if len(args) >= 2 {
			precision := strings.ToLower(strings.Trim(toString(args[0]), "'"))
			t, err := parseTime(toString(args[1]))
			if err == nil {
				switch precision {
				case "year":
					return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
				case "month":
					return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
				case "day":
					return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location()).Format(time.RFC3339)
				case "hour":
					return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location()).Format(time.RFC3339)
				}
			}
		}
	case "EXTRACT":
		// EXTRACT(field FROM timestamp) — simplified: treat args[0] as field, args[1] as value
		if len(args) >= 2 {
			field := strings.ToLower(strings.Trim(toString(args[0]), "'"))
			t, err := parseTime(toString(args[1]))
			if err == nil {
				switch field {
				case "year":
					return float64(t.Year())
				case "month":
					return float64(t.Month())
				case "day":
					return float64(t.Day())
				case "hour":
					return float64(t.Hour())
				case "minute":
					return float64(t.Minute())
				case "second":
					return float64(t.Second())
				case "dow":
					return float64(t.Weekday())
				case "epoch":
					return float64(t.Unix())
				}
			}
		}
	case "AGE":
		if len(args) >= 1 {
			t1, err1 := parseTime(toString(args[0]))
			if err1 == nil {
				ref := time.Now()
				if len(args) == 2 {
					t2, err2 := parseTime(toString(args[1]))
					if err2 == nil {
						ref = t2
					}
				}
				dur := ref.Sub(t1)
				return dur.String()
			}
		}
	case "TO_DATE":
		if len(args) >= 1 {
			return toString(args[0]) // simplified
		}
	case "TO_TIMESTAMP":
		if len(args) >= 1 {
			return toString(args[0]) // simplified
		}

	// ---- Type casting ----
	case "CAST":
		// CAST(expr AS type) — the args are already partially evaluated
		// This branch usually handled by evaluateCast but keep as fallback
		if len(args) >= 1 {
			return args[0]
		}
	}

	return nil
}

// evaluateCast handles CAST(expr AS TYPE) expressions
func evaluateCast(expr string, row Row) interface{} {
	// Remove CAST( prefix and trailing )
	inner := expr[5 : len(expr)-1]

	// Find " AS " keyword (case-insensitive)
	upper := strings.ToUpper(inner)
	asIdx := strings.LastIndex(upper, " AS ")
	if asIdx < 0 {
		return nil
	}

	valueExpr := strings.TrimSpace(inner[:asIdx])
	typeName := strings.ToUpper(strings.TrimSpace(inner[asIdx+4:]))

	val := EvaluateExpression(valueExpr, row)

	switch {
	case typeName == "TEXT" || typeName == "VARCHAR" || strings.HasPrefix(typeName, "VARCHAR(") || typeName == "CHAR":
		return toString(val)
	case typeName == "INT" || typeName == "INTEGER" || typeName == "BIGINT" || typeName == "SMALLINT":
		if f, err := ConvertToFloat64(val); err == nil {
			return int(f)
		}
		if s, ok := val.(string); ok {
			if i, err := strconv.Atoi(s); err == nil {
				return i
			}
		}
		return 0
	case typeName == "FLOAT" || typeName == "FLOAT4" || typeName == "FLOAT8" || typeName == "DOUBLE PRECISION" || typeName == "NUMERIC" || typeName == "DECIMAL":
		if f, err := ConvertToFloat64(val); err == nil {
			return f
		}
		return 0.0
	case typeName == "BOOLEAN" || typeName == "BOOL":
		s := strings.ToLower(toString(val))
		return s == "true" || s == "1" || s == "yes" || s == "t"
	default:
		return val
	}
}

// splitArgs splits a comma-separated argument string, evaluating each arg
func splitArgs(argsStr string, row Row) []interface{} {
	raw := splitRawArgs(argsStr)
	result := make([]interface{}, len(raw))
	for i, r := range raw {
		r = strings.TrimSpace(r)
		result[i] = EvaluateExpression(r, row)
	}
	return result
}

// splitRawArgs splits on commas that are not inside parentheses
func splitRawArgs(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// toString converts any value to its string representation
func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case int:
		return strconv.Itoa(val)
	case float64:
		if val == math.Trunc(val) {
			return strconv.FormatInt(int64(val), 10)
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		return strconv.FormatFloat(func() float64 {
			f, _ := ConvertToFloat64(v)
			return f
		}(), 'f', -1, 64)
	}
}

// convToInt converts any value to int
func convToInt(v interface{}) int {
	switch val := v.(type) {
	case int:
		return val
	case float64:
		return int(val)
	case string:
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return int(f)
		}
	}
	return 0
}

// parseTime attempts to parse various time string formats
func parseTime(s string) (time.Time, error) {
	formats := []string{
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
		"2006-01-02T15:04:05",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, strconv.ErrSyntax
}

// ApplyOperator applies an arithmetic operator to two values
func ApplyOperator(left interface{}, op string, right interface{}) interface{} {
	l, errL := ConvertToFloat64(left)
	r, errR := ConvertToFloat64(right)
	if errL != nil || errR != nil {
		// String concatenation for +
		if op == "+" {
			return toString(left) + toString(right)
		}
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

// EvaluateCondition evaluates a boolean condition string
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

// applyTypeCast applies PostgreSQL-style :: type casting
func applyTypeCast(val interface{}, castType string) interface{} {
	if val == nil {
		return nil
	}

	strVal := strings.TrimSpace(toString(val))
	if strings.HasPrefix(strVal, "'") && strings.HasSuffix(strVal, "'") {
		strVal = strVal[1 : len(strVal)-1]
	}

	castType = strings.ToUpper(strings.TrimSpace(castType))

	switch {
	case strings.Contains(castType, "INT"):
		if i, err := strconv.ParseInt(strVal, 10, 64); err == nil {
			return int(i)
		}
		if f, err := strconv.ParseFloat(strVal, 64); err == nil {
			return int(f)
		}
		return nil
	case strings.Contains(castType, "FLOAT") || strings.Contains(castType, "DOUBLE") || strings.Contains(castType, "NUMERIC") || strings.Contains(castType, "DECIMAL"):
		if f, err := strconv.ParseFloat(strVal, 64); err == nil {
			return f
		}
		return nil
	case castType == "DATE" || castType == "TIMESTAMP":
		if t, err := parseTime(strVal); err == nil {
			if castType == "DATE" {
				return t.Format("2006-01-02")
			}
			return t.Format(time.RFC3339)
		}
		return nil
	case castType == "BOOLEAN" || castType == "BOOL":
		lower := strings.ToLower(strVal)
		if lower == "true" || lower == "t" || lower == "1" || lower == "yes" || lower == "y" {
			return true
		}
		if lower == "false" || lower == "f" || lower == "0" || lower == "no" || lower == "n" {
			return false
		}
		return nil
	case strings.Contains(castType, "CHAR") || strings.Contains(castType, "TEXT"):
		return strVal
	}

	// Default fallback: return as string
	return strVal
}

func evaluateJsonExtract(lVal interface{}, key string, asText bool) interface{} {
	if lVal == nil {
		return nil
	}

	var data interface{}
	switch val := lVal.(type) {
	case string:
		s := val
		if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") && len(s) >= 2 {
			s = s[1 : len(s)-1]
		}
		if err := json.Unmarshal([]byte(s), &data); err != nil {
			return nil
		}
	default:
		data = val
	}

	var result interface{}
	switch m := data.(type) {
	case map[string]interface{}:
		result = m[key]
	case []interface{}:
		if idx, err := strconv.Atoi(key); err == nil && idx >= 0 && idx < len(m) {
			result = m[idx]
		}
	}

	if result == nil {
		return nil
	}

	if asText {
		switch r := result.(type) {
		case string:
			return r
		case float64:
			return strconv.FormatFloat(r, 'f', -1, 64)
		case bool:
			return strconv.FormatBool(r)
		default:
			bytes, _ := json.Marshal(r)
			return string(bytes)
		}
	}

	switch r := result.(type) {
	case map[string]interface{}, []interface{}:
		bytes, _ := json.Marshal(r)
		return string(bytes)
	default:
		return r
	}
}

func EvaluateJsonContain(lVal, rVal interface{}) bool {
	if lVal == nil || rVal == nil {
		return false
	}

	var left interface{}
	switch val := lVal.(type) {
	case string:
		s := val
		if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") && len(s) >= 2 {
			s = s[1 : len(s)-1]
		}
		if err := json.Unmarshal([]byte(s), &left); err != nil {
			left = s
		}
	default:
		left = val
	}

	var right interface{}
	switch val := rVal.(type) {
	case string:
		s := val
		if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") && len(s) >= 2 {
			s = s[1 : len(s)-1]
		}
		if err := json.Unmarshal([]byte(s), &right); err != nil {
			right = s
		}
	default:
		right = val
	}

	return jsonContains(left, right)
}

func jsonContains(left, right interface{}) bool {
	if left == nil && right == nil {
		return true
	}
	if left == nil || right == nil {
		return false
	}

	switch r := right.(type) {
	case map[string]interface{}:
		l, ok := left.(map[string]interface{})
		if !ok {
			return false
		}
		for k, v := range r {
			lv, exists := l[k]
			if !exists || !jsonContains(lv, v) {
				return false
			}
		}
		return true

	case []interface{}:
		l, ok := left.([]interface{})
		if !ok {
			return false
		}
		for _, rv := range r {
			found := false
			for _, lv := range l {
				if jsonContains(lv, rv) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
		return true

	default:
		return toString(left) == toString(right)
	}
}

func evaluateJsonPath(lVal interface{}, path string) interface{} {
	if lVal == nil {
		return nil
	}

	var data interface{}
	switch val := lVal.(type) {
	case string:
		s := val
		if strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'") && len(s) >= 2 {
			s = s[1 : len(s)-1]
		}
		if err := json.Unmarshal([]byte(s), &data); err != nil {
			return nil
		}
	default:
		data = val
	}

	path = strings.TrimSpace(path)
	if path == "" || path == "$" {
		return data
	}

	if strings.HasPrefix(path, "$.") {
		path = path[2:]
	} else if strings.HasPrefix(path, "$") {
		path = path[1:]
	}

	parts := strings.Split(path, ".")
	curr := data
	for _, part := range parts {
		if curr == nil {
			return nil
		}

		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		arrayIdx := -1
		if idxStart := strings.Index(part, "["); idxStart > 0 {
			if idxEnd := strings.Index(part, "]"); idxEnd > idxStart {
				idxStr := part[idxStart+1 : idxEnd]
				if idx, err := strconv.Atoi(idxStr); err == nil {
					arrayIdx = idx
				}
				part = part[:idxStart]
			}
		}

		m, ok := curr.(map[string]interface{})
		if !ok {
			return nil
		}
		curr = m[part]

		if arrayIdx >= 0 {
			s, ok := curr.([]interface{})
			if !ok || arrayIdx >= len(s) {
				return nil
			}
			curr = s[arrayIdx]
		}
	}

	return curr
}
