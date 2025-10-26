package metadata

// ObjectType identifies what the metadata belongs to
type ObjectType uint8

const (
	ObjTypeInvalid ObjectType = iota
	ObjTypeDatabase
	ObjTypeTable
	ObjTypeColumn
	ObjTypeIndex
	ObjTypeVector
)

// Metadata represents the list-based metadata format
// Index 0: Purpose
// Index 1: Description
type Metadata struct {
	ObjectType  ObjectType
	ObjectID    [16]byte // UUID of the object
	Purpose     string   // Index 0 in the list
	Description string   // Index 1 in the list
}

// NewMetadata creates a new metadata entry
func NewMetadata(objType ObjectType, objectID [16]byte, purpose, description string) *Metadata {
	return &Metadata{
		ObjectType:  objType,
		ObjectID:    objectID,
		Purpose:     purpose,
		Description: description,
	}
}

// AsList returns metadata as a list [purpose, description]
func (m *Metadata) AsList() []string {
	return []string{m.Purpose, m.Description}
}
