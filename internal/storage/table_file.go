package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ghosecorp/ghostsql/internal/metadata"
)

const (
	TableFileMagic   = "GTBL" // GhostSQL Table
	TableFileVersion = 1
)

// TableFileHeader represents the table file header
type TableFileHeader struct {
	Magic      [4]byte
	Version    uint32
	NumColumns uint16
	NumPages   uint32
	RootPageID uint64
}

// SaveTableBinary saves table to binary format
func (db *Database) SaveTableBinary(table *Table) error {
	tablePath := filepath.Join(db.DataDir.DatabasesPath, "default", "tables", table.Name+".tbl")

	// Ensure directory exists
	dir := filepath.Dir(tablePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create table directory: %w", err)
	}

	// Create file
	file, err := os.Create(tablePath)
	if err != nil {
		return fmt.Errorf("failed to create table file: %w", err)
	}
	defer file.Close()

	// Write header
	header := make([]byte, 64) // Reserve 64 bytes for header
	copy(header[0:4], TableFileMagic)
	binary.LittleEndian.PutUint32(header[4:8], TableFileVersion)
	binary.LittleEndian.PutUint16(header[8:10], uint16(len(table.Columns)))
	binary.LittleEndian.PutUint32(header[10:14], uint32(len(table.Pages)))

	if _, err := file.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write schema (columns)
	for _, col := range table.Columns {
		// Column name length + name
		nameBytes := []byte(col.Name)
		if err := binary.Write(file, binary.LittleEndian, uint16(len(nameBytes))); err != nil {
			return err
		}
		if _, err := file.Write(nameBytes); err != nil {
			return err
		}

		// Column type
		if err := binary.Write(file, binary.LittleEndian, uint8(col.Type)); err != nil {
			return err
		}

		// Nullable flag
		nullable := uint8(0)
		if col.Nullable {
			nullable = 1
		}
		if err := binary.Write(file, binary.LittleEndian, nullable); err != nil {
			return err
		}
	}

	// Write pages
	for _, page := range table.Pages {
		if _, err := file.Write(page.Data[:]); err != nil {
			return fmt.Errorf("failed to write page: %w", err)
		}
	}

	// Also save metadata separately
	if table.Metadata != nil {
		metaPath := filepath.Join(db.DataDir.MetadataPath, table.Name+".meta")
		metaData := fmt.Sprintf("%s\n%s\n", table.Metadata.Purpose, table.Metadata.Description)
		if err := os.WriteFile(metaPath, []byte(metaData), 0644); err != nil {
			return fmt.Errorf("failed to write metadata: %w", err)
		}
	}

	db.Logger.Info("Saved table %s to binary format (%d pages)", table.Name, len(table.Pages))
	return nil
}

// LoadTableBinary loads table from binary format
func (db *Database) LoadTableBinary(tableName string) (*Table, error) {
	tablePath := filepath.Join(db.DataDir.DatabasesPath, "default", "tables", tableName+".tbl")

	file, err := os.Open(tablePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open table file: %w", err)
	}
	defer file.Close()

	// Read header
	header := make([]byte, 64)
	if _, err := file.Read(header); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	// Verify magic
	magic := string(header[0:4])
	if magic != TableFileMagic {
		return nil, fmt.Errorf("invalid table file magic: %s", magic)
	}

	// Read header fields
	version := binary.LittleEndian.Uint32(header[4:8])
	if version != TableFileVersion {
		return nil, fmt.Errorf("unsupported table file version: %d", version)
	}

	numColumns := binary.LittleEndian.Uint16(header[8:10])
	numPages := binary.LittleEndian.Uint32(header[10:14])

	// Read schema
	columns := make([]Column, numColumns)
	for i := uint16(0); i < numColumns; i++ {
		// Read column name length
		var nameLen uint16
		if err := binary.Read(file, binary.LittleEndian, &nameLen); err != nil {
			return nil, err
		}

		// Read column name
		nameBytes := make([]byte, nameLen)
		if _, err := file.Read(nameBytes); err != nil {
			return nil, err
		}
		columns[i].Name = string(nameBytes)

		// Read column type
		var colType uint8
		if err := binary.Read(file, binary.LittleEndian, &colType); err != nil {
			return nil, err
		}
		columns[i].Type = DataType(colType)

		// Read nullable flag
		var nullable uint8
		if err := binary.Read(file, binary.LittleEndian, &nullable); err != nil {
			return nil, err
		}
		columns[i].Nullable = nullable == 1
	}

	// Create table
	table := &Table{
		Name:    tableName,
		Columns: columns,
		Pages:   make([]*SlottedPage, 0, numPages),
		Rows:    make([]Row, 0),
	}

	// Read pages
	for i := uint32(0); i < numPages; i++ {
		var pageData [PageSize]byte
		if _, err := file.Read(pageData[:]); err != nil {
			return nil, fmt.Errorf("failed to read page %d: %w", i, err)
		}

		page := LoadSlottedPage(pageData)
		table.Pages = append(table.Pages, page)
	}

	// Reconstruct rows from pages
	if err := table.LoadFromPages(); err != nil {
		return nil, fmt.Errorf("failed to load rows from pages: %w", err)
	}

	// Load metadata if exists
	metaPath := filepath.Join(db.DataDir.MetadataPath, tableName+".meta")
	if metaData, err := os.ReadFile(metaPath); err == nil {
		lines := string(metaData)
		var id [16]byte
		copy(id[:], tableName)
		table.Metadata = metadata.NewMetadata(
			metadata.ObjTypeTable,
			id,
			"Loaded from disk",
			lines,
		)
	}

	db.Logger.Info("Loaded table %s from binary format (%d rows, %d pages)", tableName, len(table.Rows), len(table.Pages))
	return table, nil
}
