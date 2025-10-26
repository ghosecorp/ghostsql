package storage

import (
	"encoding/binary"
	"fmt"
	"math"
)

// EncodeRow encodes a row into binary format
func EncodeRow(columns []Column, row Row) ([]byte, error) {
	// Calculate total size needed
	size := 2 // 2 bytes for column count

	for _, col := range columns {
		size += 2 // 2 bytes for null flag
		if val, exists := row[col.Name]; exists && val != nil {
			switch col.Type {
			case TypeInt:
				size += 4
			case TypeBigInt:
				size += 8
			case TypeFloat:
				size += 8
			case TypeBoolean:
				size += 1
			case TypeText, TypeVarChar:
				str := fmt.Sprintf("%v", val)
				size += 4 + len(str) // 4 bytes for length + string data
			}
		}
	}

	buf := make([]byte, size)
	offset := 0

	// Write column count
	binary.LittleEndian.PutUint16(buf[offset:], uint16(len(columns)))
	offset += 2

	// Write each column value
	for _, col := range columns {
		val, exists := row[col.Name]

		// Null flag
		if !exists || val == nil {
			binary.LittleEndian.PutUint16(buf[offset:], 1) // NULL
			offset += 2
			continue
		}

		binary.LittleEndian.PutUint16(buf[offset:], 0) // NOT NULL
		offset += 2

		// Write value based on type
		switch col.Type {
		case TypeInt:
			intVal := toInt(val)
			binary.LittleEndian.PutUint32(buf[offset:], uint32(intVal))
			offset += 4

		case TypeBigInt:
			bigIntVal := toInt64(val)
			binary.LittleEndian.PutUint64(buf[offset:], uint64(bigIntVal))
			offset += 8

		case TypeFloat:
			floatVal := toFloat64(val)
			binary.LittleEndian.PutUint64(buf[offset:], math.Float64bits(floatVal))
			offset += 8

		case TypeBoolean:
			boolVal := toBool(val)
			if boolVal {
				buf[offset] = 1
			} else {
				buf[offset] = 0
			}
			offset += 1

		case TypeText, TypeVarChar:
			str := fmt.Sprintf("%v", val)
			binary.LittleEndian.PutUint32(buf[offset:], uint32(len(str)))
			offset += 4
			copy(buf[offset:], []byte(str))
			offset += len(str)
		}
	}

	return buf[:offset], nil
}

// DecodeRow decodes binary data into a row
func DecodeRow(columns []Column, data []byte) (Row, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("invalid row data: too short")
	}

	row := make(Row)
	offset := 0

	// Read column count
	colCount := binary.LittleEndian.Uint16(data[offset:])
	offset += 2

	if int(colCount) != len(columns) {
		return nil, fmt.Errorf("column count mismatch: expected %d, got %d", len(columns), colCount)
	}

	// Read each column value
	for _, col := range columns {
		if offset+2 > len(data) {
			return nil, fmt.Errorf("unexpected end of data")
		}

		// Check null flag
		isNull := binary.LittleEndian.Uint16(data[offset:])
		offset += 2

		if isNull == 1 {
			row[col.Name] = nil
			continue
		}

		// Read value based on type
		switch col.Type {
		case TypeInt:
			if offset+4 > len(data) {
				return nil, fmt.Errorf("unexpected end of data for INT")
			}
			val := int32(binary.LittleEndian.Uint32(data[offset:]))
			row[col.Name] = int(val)
			offset += 4

		case TypeBigInt:
			if offset+8 > len(data) {
				return nil, fmt.Errorf("unexpected end of data for BIGINT")
			}
			val := int64(binary.LittleEndian.Uint64(data[offset:]))
			row[col.Name] = val
			offset += 8

		case TypeFloat:
			if offset+8 > len(data) {
				return nil, fmt.Errorf("unexpected end of data for FLOAT")
			}
			bits := binary.LittleEndian.Uint64(data[offset:])
			val := math.Float64frombits(bits)
			row[col.Name] = val
			offset += 8

		case TypeBoolean:
			if offset+1 > len(data) {
				return nil, fmt.Errorf("unexpected end of data for BOOLEAN")
			}
			row[col.Name] = data[offset] == 1
			offset += 1

		case TypeText, TypeVarChar:
			if offset+4 > len(data) {
				return nil, fmt.Errorf("unexpected end of data for string length")
			}
			strLen := binary.LittleEndian.Uint32(data[offset:])
			offset += 4

			if offset+int(strLen) > len(data) {
				return nil, fmt.Errorf("unexpected end of data for string")
			}
			str := string(data[offset : offset+int(strLen)])
			row[col.Name] = str
			offset += int(strLen)
		}
	}

	return row, nil
}

// Helper conversion functions
func toInt(val interface{}) int {
	switch v := val.(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

func toInt64(val interface{}) int64 {
	switch v := val.(type) {
	case int:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	default:
		return 0
	}
}

func toFloat64(val interface{}) float64 {
	switch v := val.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	default:
		return 0
	}
}

func toBool(val interface{}) bool {
	switch v := val.(type) {
	case bool:
		return v
	case int:
		return v != 0
	default:
		return false
	}
}
