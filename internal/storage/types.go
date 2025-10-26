package storage

// DataType represents column data types
type DataType uint8

const (
	TypeInvalid DataType = iota
	TypeInt              // INT (4 bytes)
	TypeBigInt           // BIGINT (8 bytes)
	TypeText             // TEXT (variable length)
	TypeVarChar          // VARCHAR(n) (variable length with limit)
	TypeFloat            // FLOAT (8 bytes)
	TypeBoolean          // BOOLEAN (1 byte)
	TypeVector           // VECTOR(n) (for LLM embeddings)
)

func (dt DataType) String() string {
	switch dt {
	case TypeInt:
		return "INT"
	case TypeBigInt:
		return "BIGINT"
	case TypeText:
		return "TEXT"
	case TypeVarChar:
		return "VARCHAR"
	case TypeFloat:
		return "FLOAT"
	case TypeBoolean:
		return "BOOLEAN"
	case TypeVector:
		return "VECTOR"
	default:
		return "INVALID"
	}
}

// IsFixedSize returns true if the type has a fixed size
func (dt DataType) IsFixedSize() bool {
	switch dt {
	case TypeInt, TypeBigInt, TypeFloat, TypeBoolean:
		return true
	default:
		return false
	}
}

// FixedSize returns the size in bytes for fixed-size types
func (dt DataType) FixedSize() int {
	switch dt {
	case TypeInt:
		return 4
	case TypeBigInt:
		return 8
	case TypeFloat:
		return 8
	case TypeBoolean:
		return 1
	default:
		return 0
	}
}

// PageType represents different page types
type PageType uint8

const (
	PageTypeInvalid PageType = iota
	PageTypeData             // Regular data pages
	PageTypeIndex            // B+tree index pages
	PageTypeVector           // Vector index pages
	PageTypeMeta             // Metadata pages
)

const (
	PageSize = 16384 // 16KB pages (like PostgreSQL)
)
