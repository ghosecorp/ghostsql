package metadata

import (
	"encoding/binary"
	"fmt"
	"os"
)

const (
	MetadataFileMagic = "GMDB" // GhostSQL Metadata DB
	MetadataVersion   = 1
	HeaderSize        = 32
	EntryHeaderSize   = 56
)

// MetadataStore manages metadata storage
type MetadataStore struct {
	FilePath string
	file     *os.File
}

// OpenMetadataStore opens or creates a metadata store
func OpenMetadataStore(filepath string) (*MetadataStore, error) {
	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open metadata file: %w", err)
	}

	ms := &MetadataStore{
		FilePath: filepath,
		file:     file,
	}

	// Initialize if new file
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	if stat.Size() == 0 {
		if err := ms.initializeFile(); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to initialize metadata file: %w", err)
		}
	}

	return ms, nil
}

// initializeFile creates the initial file header
func (ms *MetadataStore) initializeFile() error {
	header := make([]byte, HeaderSize)

	// Magic number
	copy(header[0:4], MetadataFileMagic)

	// Version
	binary.LittleEndian.PutUint32(header[4:8], MetadataVersion)

	// NumEntries (0 initially)
	binary.LittleEndian.PutUint64(header[8:16], 0)

	// IndexOffset (starts right after header)
	binary.LittleEndian.PutUint64(header[16:24], HeaderSize)

	// DataOffset
	binary.LittleEndian.PutUint64(header[24:32], HeaderSize)

	if _, err := ms.file.WriteAt(header, 0); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	return ms.file.Sync()
}

// Close closes the metadata store
func (ms *MetadataStore) Close() error {
	return ms.file.Close()
}
