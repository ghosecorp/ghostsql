// internal/storage/database.go
package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/ghosecorp/ghostsql/internal/metadata"
	"github.com/ghosecorp/ghostsql/internal/util"
)

// Type aliases
type Logger = util.Logger
type MetadataStore = metadata.MetadataStore

// DatabaseInstance represents a single database
type DatabaseInstance struct {
	Name     string
	Tables   map[string]*Table
	BasePath string
	mu       sync.RWMutex
}

// NewDatabaseInstance creates a new database instance
func NewDatabaseInstance(name string, basePath string) *DatabaseInstance {
	return &DatabaseInstance{
		Name:     name,
		Tables:   make(map[string]*Table),
		BasePath: basePath,
	}
}

// Database represents the GhostSQL server managing multiple databases
type Database struct {
	DataDir         *DataDir
	Logger          *util.Logger
	MetadataStore   *metadata.MetadataStore
	Databases       map[string]*DatabaseInstance
	CurrentDatabase string
	LockFile        string
	mu              sync.RWMutex
}

// Initialize sets up the database with persistent storage
func Initialize() (*Database, error) {
	logger := util.NewLogger("GhostSQL")

	// Initialize data directory structure
	logger.Info("Initializing data directory...")
	dd, err := InitDataDirectory()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize data directory: %w", err)
	}

	db := &Database{
		DataDir:   dd,
		Logger:    logger,
		Databases: make(map[string]*DatabaseInstance),
		LockFile:  filepath.Join(dd.RootPath, "ghostsql.pid"),
	}

	// Acquire lock file
	if err := db.acquireLock(); err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}

	// Load existing databases
	logger.Info("Loading databases from disk...")
	if err := db.LoadAllDatabases(); err != nil {
		logger.Error("Failed to load databases: %v", err)
	}

	// Create default database if none exist
	if len(db.Databases) == 0 {
		logger.Info("No databases found, creating default database...")
		if err := db.CreateDatabase("ghostsql"); err != nil {
			return nil, fmt.Errorf("failed to create default database: %w", err)
		}
	}

	logger.Info("Database initialized at: %s", dd.RootPath)
	logger.Info("Loaded %d database(s)", len(db.Databases))
	return db, nil
}

// CreateDatabase creates a new database
func (db *Database) CreateDatabase(dbName string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.Databases[dbName]; exists {
		return fmt.Errorf("database %s already exists", dbName)
	}

	// Create database directory
	dbPath := filepath.Join(db.DataDir.DatabasesPath, dbName)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	// Create subdirectories
	dirs := []string{
		filepath.Join(dbPath, "tables"),
		filepath.Join(dbPath, "indexes"),
		filepath.Join(dbPath, "vectors"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	}

	// Create database instance
	dbInstance := NewDatabaseInstance(dbName, dbPath)
	db.Databases[dbName] = dbInstance

	// If this is the first database, make it current
	if db.CurrentDatabase == "" {
		db.CurrentDatabase = dbName
	}

	db.Logger.Info("Created database: %s", dbName)
	return nil
}

// UseDatabase switches to a different database
func (db *Database) UseDatabase(dbName string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, exists := db.Databases[dbName]; !exists {
		return fmt.Errorf("database %s does not exist", dbName)
	}

	db.CurrentDatabase = dbName
	db.Logger.Info("Switched to database: %s", dbName)
	return nil
}

// GetCurrentDatabase returns the current database instance
func (db *Database) GetCurrentDatabase() (*DatabaseInstance, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if db.CurrentDatabase == "" {
		return nil, fmt.Errorf("no database selected")
	}

	dbInstance, exists := db.Databases[db.CurrentDatabase]
	if !exists {
		return nil, fmt.Errorf("current database %s does not exist", db.CurrentDatabase)
	}

	return dbInstance, nil
}

// ListDatabases returns all database names
func (db *Database) ListDatabases() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	names := make([]string, 0, len(db.Databases))
	for name := range db.Databases {
		names = append(names, name)
	}
	return names
}

// DropDatabase deletes a database
func (db *Database) DropDatabase(dbName string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if dbName == db.CurrentDatabase {
		return fmt.Errorf("cannot drop currently selected database")
	}

	dbInstance, exists := db.Databases[dbName]
	if !exists {
		return fmt.Errorf("database %s does not exist", dbName)
	}

	// Remove directory
	if err := os.RemoveAll(dbInstance.BasePath); err != nil {
		return fmt.Errorf("failed to remove database directory: %w", err)
	}

	delete(db.Databases, dbName)
	db.Logger.Info("Dropped database: %s", dbName)
	return nil
}

// LoadAllDatabases loads all databases from disk
func (db *Database) LoadAllDatabases() error {
	entries, err := os.ReadDir(db.DataDir.DatabasesPath)
	if err != nil {
		return fmt.Errorf("failed to read databases directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		dbName := entry.Name()
		dbPath := filepath.Join(db.DataDir.DatabasesPath, dbName)

		// Create database instance
		dbInstance := NewDatabaseInstance(dbName, dbPath)

		// Load tables for this database
		if err := db.loadTablesForDatabase(dbInstance); err != nil {
			db.Logger.Error("Failed to load tables for database %s: %v", dbName, err)
			continue
		}

		db.Databases[dbName] = dbInstance

		// Set first database as current
		if db.CurrentDatabase == "" {
			db.CurrentDatabase = dbName
		}
	}

	return nil
}

// loadTablesForDatabase loads all tables for a specific database
func (db *Database) loadTablesForDatabase(dbInstance *DatabaseInstance) error {
	tablesDir := filepath.Join(dbInstance.BasePath, "tables")

	if _, err := os.Stat(tablesDir); os.IsNotExist(err) {
		return nil
	}

	entries, err := os.ReadDir(tablesDir)
	if err != nil {
		return fmt.Errorf("failed to read tables directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".tbl" {
			continue
		}

		tableName := entry.Name()[:len(entry.Name())-4]

		table, err := db.loadTableForDatabase(dbInstance, tableName)
		if err != nil {
			db.Logger.Error("Failed to load table %s: %v", tableName, err)
			continue
		}

		dbInstance.Tables[tableName] = table
	}

	db.Logger.Info("Loaded %d table(s) for database %s", len(dbInstance.Tables), dbInstance.Name)
	return nil
}

// loadTableForDatabase loads a specific table
func (db *Database) loadTableForDatabase(dbInstance *DatabaseInstance, tableName string) (*Table, error) {
	tablePath := filepath.Join(dbInstance.BasePath, "tables", tableName+".tbl")
	return db.loadTableBinaryFromPath(tablePath, tableName)
}

// saveTableForDatabase saves a table for a specific database
func (db *Database) saveTableForDatabase(dbInstance *DatabaseInstance, table *Table) error {
	tablePath := filepath.Join(dbInstance.BasePath, "tables", table.Name+".tbl")
	return db.saveTableBinaryToPath(table, tablePath)
}

// acquireLock creates a lock file
func (db *Database) acquireLock() error {
	lockFile, err := os.OpenFile(db.LockFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("database is already running (lock file exists)")
		}
		return fmt.Errorf("failed to create lock file: %w", err)
	}
	defer lockFile.Close()

	pid := os.Getpid()
	if _, err := lockFile.WriteString(fmt.Sprintf("%d\n", pid)); err != nil {
		return fmt.Errorf("failed to write PID to lock file: %w", err)
	}

	return nil
}

// Shutdown cleanly shuts down the database
func (db *Database) Shutdown() error {
	db.Logger.Info("Shutting down database...")

	// Save all tables in all databases
	db.Logger.Info("Saving all databases...")
	for _, dbInstance := range db.Databases {
		for _, table := range dbInstance.Tables {
			if err := db.saveTableForDatabase(dbInstance, table); err != nil {
				db.Logger.Error("Failed to save table %s: %v", table.Name, err)
			}
		}
	}

	// Remove lock file
	if err := os.Remove(db.LockFile); err != nil {
		db.Logger.Error("Failed to remove lock file: %v", err)
	}

	db.Logger.Info("Database shutdown complete")
	return nil
}
