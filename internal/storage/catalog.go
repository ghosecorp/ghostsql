package storage

import (
	"hash/fnv"
)

// CatalogProvider provides virtualized system tables for pg_catalog
type CatalogProvider struct {
	db *Database
}

// NewCatalogProvider creates a new catalog provider
func NewCatalogProvider(db *Database) *CatalogProvider {
	return &CatalogProvider{db: db}
}

// GenerateOID creates a deterministic OID for a string name
func (cp *CatalogProvider) GenerateOID(name string) int64 {
	h := fnv.New32a()
	h.Write([]byte(name))
	return int64(h.Sum32())
}

// GetPGClassRows returns rows for pg_catalog.pg_class
func (cp *CatalogProvider) GetPGClassRows(dbInstance *DatabaseInstance) []Row {
	rows := make([]Row, 0)

	dbInstance.mu.RLock()
	defer dbInstance.mu.RUnlock()

	for name, table := range dbInstance.Tables {
		rows = append(rows, Row{
			"oid":           cp.GenerateOID(name),
			"relname":       name,
			"relnamespace":  cp.GenerateOID("public"), // Default namespace OID
			"reltype":       int64(0),
			"relowner":      int64(10),
			"relam":         int64(0),
			"relfilenode":   int64(0),
			"reltablespace": int64(0),
			"relpages":      int32(0),
			"reltuples":     float32(len(table.Rows)),
			"relallvisible": int32(0),
			"reltoastrelid": int64(0),
			"relhasindex":   false,
			"relisshared":   false,
			"relpersistence": "p",
			"relkind":       "r", // 'r' = ordinary table
			"relnatts":      int16(len(table.Columns)),
		})
	}

	return rows
}

// GetPGNamespaceRows returns rows for pg_catalog.pg_namespace
func (cp *CatalogProvider) GetPGNamespaceRows() []Row {
	return []Row{
		{
			"oid":      cp.GenerateOID("public"),
			"nspname":  "public",
			"nspowner": int64(10),
		},
		{
			"oid":      cp.GenerateOID("pg_catalog"),
			"nspname":  "pg_catalog",
			"nspowner": int64(10),
		},
	}
}

// GetPGAttributeRows returns rows for pg_catalog.pg_attribute
func (cp *CatalogProvider) GetPGAttributeRows(dbInstance *DatabaseInstance) []Row {
	rows := make([]Row, 0)

	dbInstance.mu.RLock()
	defer dbInstance.mu.RUnlock()

	for tableName, table := range dbInstance.Tables {
		tableOID := cp.GenerateOID(tableName)
		for i, col := range table.Columns {
			rows = append(rows, Row{
				"attrelid":  tableOID,
				"attname":   col.Name,
				"atttypid":  int64(cp.mapTypeToOID(col.Type)),
				"attlen":    int16(col.Length),
				"attnum":    int16(i + 1),
				"attndims":  int32(0),
				"atttypmod": int32(-1),
				"attnotnull": !col.Nullable,
				"atthasdef":  false,
				"attisdropped": false,
				"attislocal":   true,
				"attinhcount":  int32(0),
				"attcollation": int64(0),
			})
		}
	}

	return rows
}

func (cp *CatalogProvider) mapTypeToOID(t DataType) int64 {
	switch t {
	case TypeInt:
		return 23 // int4
	case TypeText:
		return 25 // text
	case TypeVarChar:
		return 1043 // varchar
	case TypeVector:
		return 25 // Map to text for now for compatibility
	default:
		return 25 // Default to text
	}
}

func (cp *CatalogProvider) GetPGClassColumns() []Column {
	return []Column{
		{Name: "oid", Type: TypeInt},
		{Name: "relname", Type: TypeText},
		{Name: "relnamespace", Type: TypeInt},
		{Name: "reltype", Type: TypeInt},
		{Name: "relowner", Type: TypeInt},
		{Name: "relam", Type: TypeInt},
		{Name: "relfilenode", Type: TypeInt},
		{Name: "reltablespace", Type: TypeInt},
		{Name: "relpages", Type: TypeInt},
		{Name: "reltuples", Type: TypeInt},
		{Name: "relallvisible", Type: TypeInt},
		{Name: "reltoastrelid", Type: TypeInt},
		{Name: "relhasindex", Type: TypeInt},
		{Name: "relisshared", Type: TypeInt},
		{Name: "relpersistence", Type: TypeText},
		{Name: "relkind", Type: TypeText},
		{Name: "relnatts", Type: TypeInt},
	}
}

func (cp *CatalogProvider) GetPGNamespaceColumns() []Column {
	return []Column{
		{Name: "oid", Type: TypeInt},
		{Name: "nspname", Type: TypeText},
		{Name: "nspowner", Type: TypeInt},
	}
}

func (cp *CatalogProvider) GetPGAttributeColumns() []Column {
	return []Column{
		{Name: "attrelid", Type: TypeInt},
		{Name: "attname", Type: TypeText},
		{Name: "atttypid", Type: TypeInt},
		{Name: "attlen", Type: TypeInt},
		{Name: "attnum", Type: TypeInt},
		{Name: "attndims", Type: TypeInt},
		{Name: "atttypmod", Type: TypeInt},
		{Name: "attnotnull", Type: TypeInt},
		{Name: "atthasdef", Type: TypeInt},
		{Name: "attisdropped", Type: TypeInt},
		{Name: "attislocal", Type: TypeInt},
		{Name: "attinhcount", Type: TypeInt},
		{Name: "attcollation", Type: TypeInt},
	}
}

// GetPGAttrDefRows returns rows for pg_catalog.pg_attrdef
func (cp *CatalogProvider) GetPGAttrDefRows() []Row {
	return []Row{} // Empty for now, no defaults
}

func (cp *CatalogProvider) GetPGAttrDefColumns() []Column {
	return []Column{
		{Name: "oid", Type: TypeInt},
		{Name: "adrelid", Type: TypeInt},
		{Name: "adnum", Type: TypeInt},
		{Name: "adbin", Type: TypeText},
	}
}

// GetPGTypeRows returns rows for pg_catalog.pg_type
func (cp *CatalogProvider) GetPGTypeRows() []Row {
	return []Row{
		{"oid": int64(23), "typname": "int4", "typlen": int16(4), "typnamespace": cp.GenerateOID("pg_catalog")},
		{"oid": int64(25), "typname": "text", "typlen": int16(-1), "typnamespace": cp.GenerateOID("pg_catalog")},
		{"oid": int64(1043), "typname": "varchar", "typlen": int16(-1), "typnamespace": cp.GenerateOID("pg_catalog")},
	}
}

func (cp *CatalogProvider) GetPGTypeColumns() []Column {
	return []Column{
		{Name: "oid", Type: TypeInt},
		{Name: "typname", Type: TypeText},
		{Name: "typlen", Type: TypeInt},
		{Name: "typnamespace", Type: TypeInt},
	}
}

// GetPGCollationRows returns rows for pg_catalog.pg_collation
func (cp *CatalogProvider) GetPGCollationRows() []Row {
	return []Row{
		{"oid": int64(100), "collname": "default", "collnamespace": cp.GenerateOID("pg_catalog")},
	}
}

func (cp *CatalogProvider) GetPGCollationColumns() []Column {
	return []Column{
		{Name: "oid", Type: TypeInt},
		{Name: "collname", Type: TypeText},
		{Name: "collnamespace", Type: TypeInt},
	}
}

// GetPGConstraintRows returns rows for pg_catalog.pg_constraint
func (cp *CatalogProvider) GetPGConstraintRows() []Row {
	return []Row{} // Empty for now
}

func (cp *CatalogProvider) GetPGConstraintColumns() []Column {
	return []Column{
		{Name: "oid", Type: TypeInt},
		{Name: "conname", Type: TypeText},
		{Name: "conrelid", Type: TypeInt},
		{Name: "contype", Type: TypeText},
	}
}
