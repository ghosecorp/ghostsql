package storage

// DataType represents column data types
type DataType uint8

const (
	TypeInvalid DataType = iota
	TypeInt              // INT
	TypeBigInt           // BIGINT
	TypeText             // TEXT
	TypeFloat            // FLOAT
	TypeBoolean          // BOOLEAN
	TypeVector           // VECTOR (for LLM embeddings)
)

func (dt DataType) String() string {
	switch dt {
	case TypeInt:
		return "INT"
	case TypeBigInt:
		return "BIGINT"
	case TypeText:
		return "TEXT"
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
