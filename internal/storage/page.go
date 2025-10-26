package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

// Page represents a 16KB storage page
type Page struct {
	ID       uint64
	Type     PageType
	Data     [PageSize]byte
	Dirty    bool
	PinCount int
	mu       sync.RWMutex
}

// NewPage creates a new page
func NewPage(id uint64, pageType PageType) *Page {
	return &Page{
		ID:   id,
		Type: pageType,
	}
}

// Pin increments the pin count
func (p *Page) Pin() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.PinCount++
}

// Unpin decrements the pin count
func (p *Page) Unpin() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.PinCount > 0 {
		p.PinCount--
	}
}

// MarkDirty marks the page as modified
func (p *Page) MarkDirty() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Dirty = true
}

// PageManager manages page I/O and buffering
type PageManager struct {
	file      *os.File
	pages     map[uint64]*Page
	freePages []uint64
	nextID    uint64
	mu        sync.RWMutex
}

// NewPageManager creates a new page manager
func NewPageManager(filepath string) (*PageManager, error) {
	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	pm := &PageManager{
		file:      file,
		pages:     make(map[uint64]*Page),
		freePages: make([]uint64, 0),
		nextID:    0,
	}

	// Initialize file if new
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	if stat.Size() == 0 {
		if err := pm.initializeFile(); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to initialize file: %w", err)
		}
	} else {
		// Read metadata from existing file
		if err := pm.loadMetadata(); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to load metadata: %w", err)
		}
	}

	return pm, nil
}

// initializeFile creates the initial file structure
func (pm *PageManager) initializeFile() error {
	// Write a header page (page 0)
	header := make([]byte, PageSize)

	// Magic number "GSQL"
	copy(header[0:4], "GSQL")

	// Version
	binary.LittleEndian.PutUint32(header[4:8], 1)

	// Next page ID
	binary.LittleEndian.PutUint64(header[8:16], 1)

	if _, err := pm.file.WriteAt(header, 0); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	pm.nextID = 1
	return pm.file.Sync()
}

// loadMetadata loads metadata from the header page
func (pm *PageManager) loadMetadata() error {
	header := make([]byte, PageSize)
	if _, err := pm.file.ReadAt(header, 0); err != nil {
		return fmt.Errorf("failed to read header: %w", err)
	}

	// Verify magic number
	magic := string(header[0:4])
	if magic != "GSQL" {
		return fmt.Errorf("invalid magic number: %s", magic)
	}

	// Read next page ID
	pm.nextID = binary.LittleEndian.Uint64(header[8:16])

	return nil
}

// AllocatePage allocates a new page
func (pm *PageManager) AllocatePage(pageType PageType) (*Page, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	var pageID uint64

	// Reuse free page if available
	if len(pm.freePages) > 0 {
		pageID = pm.freePages[len(pm.freePages)-1]
		pm.freePages = pm.freePages[:len(pm.freePages)-1]
	} else {
		pageID = pm.nextID
		pm.nextID++
	}

	page := NewPage(pageID, pageType)
	pm.pages[pageID] = page

	// Update header with new nextID
	header := make([]byte, 16)
	copy(header[0:4], "GSQL")
	binary.LittleEndian.PutUint32(header[4:8], 1)
	binary.LittleEndian.PutUint64(header[8:16], pm.nextID)

	if _, err := pm.file.WriteAt(header, 0); err != nil {
		return nil, fmt.Errorf("failed to update header: %w", err)
	}

	return page, nil
}

// ReadPage reads a page from disk
func (pm *PageManager) ReadPage(pageID uint64) (*Page, error) {
	pm.mu.RLock()
	// Check if page is already in memory
	if page, exists := pm.pages[pageID]; exists {
		pm.mu.RUnlock()
		return page, nil
	}
	pm.mu.RUnlock()

	// Read from disk
	page := &Page{ID: pageID}
	offset := int64(pageID) * PageSize

	if _, err := pm.file.ReadAt(page.Data[:], offset); err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", pageID, err)
	}

	// Determine page type from data
	page.Type = PageType(page.Data[0])

	pm.mu.Lock()
	pm.pages[pageID] = page
	pm.mu.Unlock()

	return page, nil
}

// WritePage writes a page to disk
func (pm *PageManager) WritePage(page *Page) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	offset := int64(page.ID) * PageSize

	// Write page type at the beginning
	page.Data[0] = byte(page.Type)

	if _, err := pm.file.WriteAt(page.Data[:], offset); err != nil {
		return fmt.Errorf("failed to write page %d: %w", page.ID, err)
	}

	page.Dirty = false
	return nil
}

// FlushAll flushes all dirty pages to disk
func (pm *PageManager) FlushAll() error {
	pm.mu.RLock()
	dirtyPages := make([]*Page, 0)
	for _, page := range pm.pages {
		if page.Dirty {
			dirtyPages = append(dirtyPages, page)
		}
	}
	pm.mu.RUnlock()

	for _, page := range dirtyPages {
		if err := pm.WritePage(page); err != nil {
			return fmt.Errorf("failed to flush page %d: %w", page.ID, err)
		}
	}

	return pm.file.Sync()
}

// FreePage marks a page as free for reuse
func (pm *PageManager) FreePage(pageID uint64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	delete(pm.pages, pageID)
	pm.freePages = append(pm.freePages, pageID)
}

// Close closes the page manager
func (pm *PageManager) Close() error {
	// Flush all dirty pages
	if err := pm.FlushAll(); err != nil {
		return fmt.Errorf("failed to flush pages: %w", err)
	}

	return pm.file.Close()
}
