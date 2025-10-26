package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

// DataDir manages the persistent storage directory structure
type DataDir struct {
	RootPath      string
	DatabasesPath string
	WALPath       string
	VectorsPath   string
	MetadataPath  string
	TempPath      string
}

// InitDataDirectory initializes the data directory structure
func InitDataDirectory() (*DataDir, error) {
	// Get executable directory
	ex, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("failed to get executable path: %w", err)
	}
	exePath := filepath.Dir(ex)

	// Create data directory relative to executable
	dataRoot := filepath.Join(exePath, "data")

	dd := &DataDir{
		RootPath:      dataRoot,
		DatabasesPath: filepath.Join(dataRoot, "databases"),
		WALPath:       filepath.Join(dataRoot, "wal"),
		VectorsPath:   filepath.Join(dataRoot, "vectors"),
		MetadataPath:  filepath.Join(dataRoot, "metadata"),
		TempPath:      filepath.Join(dataRoot, "temp"),
	}

	// Create all necessary directories
	dirs := []string{
		dd.RootPath,
		dd.DatabasesPath,
		dd.WALPath,
		dd.VectorsPath,
		dd.MetadataPath,
		dd.TempPath,
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	return dd, nil
}

// CreateDatabaseDirectory creates storage for a specific database
func (dd *DataDir) CreateDatabaseDirectory(dbName string) (string, error) {
	dbPath := filepath.Join(dd.DatabasesPath, dbName)

	// Create database subdirectories
	dirs := []string{
		dbPath,
		filepath.Join(dbPath, "tables"),
		filepath.Join(dbPath, "indexes"),
		filepath.Join(dbPath, "vectors"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return "", fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	return dbPath, nil
}

// GetTableFilePath returns the file path for a table
func (dd *DataDir) GetTableFilePath(dbName, tableName string) string {
	return filepath.Join(dd.DatabasesPath, dbName, "tables", tableName+".tbl")
}

// GetMetadataFilePath returns the metadata file path for a database
func (dd *DataDir) GetMetadataFilePath(dbName string) string {
	return filepath.Join(dd.MetadataPath, dbName+".meta")
}
