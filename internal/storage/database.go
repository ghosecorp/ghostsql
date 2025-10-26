package storage

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ghosecorp/ghostsql/internal/metadata"
	"github.com/ghosecorp/ghostsql/internal/util"
)

// Database represents the GhostSQL database instance
type Database struct {
	DataDir       *DataDir
	Logger        *util.Logger
	MetadataStore *metadata.MetadataStore
	Tables        map[string]*Table
	LockFile      string
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
		DataDir:  dd,
		Logger:   logger,
		Tables:   make(map[string]*Table),
		LockFile: filepath.Join(dd.RootPath, "ghostsql.pid"),
	}

	// Acquire lock file
	if err := db.acquireLock(); err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}

	// Load existing tables from disk
	logger.Info("Loading tables from disk...")
	if err := db.LoadAllTables(); err != nil {
		logger.Error("Failed to load tables: %v", err)
	}

	logger.Info("Database initialized at: %s", dd.RootPath)
	logger.Info("Loaded %d table(s)", len(db.Tables))
	return db, nil
}

// acquireLock creates a lock file to prevent multiple instances
func (db *Database) acquireLock() error {
	lockFile, err := os.OpenFile(db.LockFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("database is already running (lock file exists)")
		}
		return fmt.Errorf("failed to create lock file: %w", err)
	}
	defer lockFile.Close()

	// Write PID to lock file
	pid := os.Getpid()
	if _, err := lockFile.WriteString(fmt.Sprintf("%d\n", pid)); err != nil {
		return fmt.Errorf("failed to write PID to lock file: %w", err)
	}

	return nil
}

// Shutdown cleanly shuts down the database
func (db *Database) Shutdown() error {
	db.Logger.Info("Shutting down database...")

	// Save all tables to disk
	db.Logger.Info("Saving tables to disk...")
	if err := db.SaveAllTables(); err != nil {
		db.Logger.Error("Failed to save tables: %v", err)
	}

	// Remove lock file
	if err := os.Remove(db.LockFile); err != nil {
		db.Logger.Error("Failed to remove lock file: %v", err)
	}

	db.Logger.Info("Database shutdown complete")
	return nil
}
