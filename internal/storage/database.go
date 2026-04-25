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
	db       *Database
}

// GetTable retrieves a table by name safely, including virtual system tables
func (di *DatabaseInstance) GetTable(name string) (*Table, bool) {
	// Handle pg_catalog virtualization
	if name == "pg_class" || name == "pg_catalog.pg_class" {
		rows := di.db.Catalog.GetPGClassRows(di)
		return &Table{Name: "pg_class", Rows: rows, Columns: di.db.Catalog.GetPGClassColumns()}, true
	}
	if name == "pg_namespace" || name == "pg_catalog.pg_namespace" {
		rows := di.db.Catalog.GetPGNamespaceRows()
		return &Table{Name: "pg_namespace", Rows: rows, Columns: di.db.Catalog.GetPGNamespaceColumns()}, true
	}
	if name == "pg_attribute" || name == "pg_catalog.pg_attribute" {
		rows := di.db.Catalog.GetPGAttributeRows(di)
		return &Table{Name: "pg_attribute", Rows: rows, Columns: di.db.Catalog.GetPGAttributeColumns()}, true
	}
	if name == "pg_attrdef" || name == "pg_catalog.pg_attrdef" {
		rows := di.db.Catalog.GetPGAttrDefRows()
		return &Table{Name: "pg_attrdef", Rows: rows, Columns: di.db.Catalog.GetPGAttrDefColumns()}, true
	}
	if name == "pg_type" || name == "pg_catalog.pg_type" {
		rows := di.db.Catalog.GetPGTypeRows()
		return &Table{Name: "pg_type", Rows: rows, Columns: di.db.Catalog.GetPGTypeColumns()}, true
	}
	if name == "pg_collation" || name == "pg_catalog.pg_collation" {
		rows := di.db.Catalog.GetPGCollationRows()
		return &Table{Name: "pg_collation", Rows: rows, Columns: di.db.Catalog.GetPGCollationColumns()}, true
	}
	if name == "pg_constraint" || name == "pg_catalog.pg_constraint" {
		rows := di.db.Catalog.GetPGConstraintRows()
		return &Table{Name: "pg_constraint", Rows: rows, Columns: di.db.Catalog.GetPGConstraintColumns()}, true
	}

	di.mu.RLock()
	defer di.mu.RUnlock()
	t, ok := di.Tables[name]
	return t, ok
}

// SetTable adds or updates a table safely
func (di *DatabaseInstance) SetTable(name string, table *Table) {
	di.mu.Lock()
	defer di.mu.Unlock()
	di.Tables[name] = table
}

// DeleteTable removes a table safely
func (di *DatabaseInstance) DeleteTable(name string) {
	di.mu.Lock()
	defer di.mu.Unlock()
	delete(di.Tables, name)
}

// NewDatabaseInstance creates a new database instance
func NewDatabaseInstance(name string, basePath string, db *Database) *DatabaseInstance {
	return &DatabaseInstance{
		Name:     name,
		Tables:   make(map[string]*Table),
		BasePath: basePath,
		db:       db,
	}
}

// DatabaseConfig stores server configuration including credentials
type DatabaseConfig struct {
	Username string
	Password string
}

// Database represents the GhostSQL server managing multiple databases
type Database struct {
	DataDir       *DataDir
	Logger        *util.Logger
	MetadataStore *metadata.MetadataStore
	Databases     map[string]*DatabaseInstance
	LockFile      string
	mu            sync.RWMutex
	SessionMgr    *SessionManager
	Catalog       *CatalogProvider
	Config        DatabaseConfig
}

// Initialize sets up the database with persistent storage
func Initialize(rootPath string) (*Database, error) {
	logger := util.NewLogger("GhostSQL")

	// Initialize data directory structure
	logger.Info("Initializing data directory...")
	dd, err := InitDataDirectory(rootPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize data directory: %w", err)
	}

	db := &Database{
		DataDir:    dd,
		Logger:     logger,
		Databases:  make(map[string]*DatabaseInstance),
		LockFile:   filepath.Join(dd.RootPath, "ghostsql.pid"),
		SessionMgr: NewSessionManager(),
		Config: DatabaseConfig{
			Username: "ghost",
			Password: "ghostsql",
		},
	}
	db.Catalog = NewCatalogProvider(db)

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
	dbInstance := NewDatabaseInstance(dbName, dbPath, db)
	db.Databases[dbName] = dbInstance

	db.Logger.Info("Created database: %s", dbName)
	return nil
}

// GetDatabaseInstance returns a database instance by name
func (db *Database) GetDatabaseInstance(dbName string) (*DatabaseInstance, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	dbInstance, exists := db.Databases[dbName]
	if !exists {
		return nil, fmt.Errorf("database %s does not exist", dbName)
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
	// Check if exists
	db.mu.RLock()
	dbInstance, exists := db.Databases[dbName]
	db.mu.RUnlock()

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
		dbInstance := NewDatabaseInstance(dbName, dbPath, db)

		// Load tables for this database
		if err := db.loadTablesForDatabase(dbInstance); err != nil {
			db.Logger.Error("Failed to load tables for database %s: %v", dbName, err)
			continue
		}

		db.Databases[dbName] = dbInstance
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
