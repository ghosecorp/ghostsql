package storage

import (
	"encoding/binary"
	"fmt"
)

// SlottedPage manages multiple rows in a single page
type SlottedPage struct {
	PageID    uint64
	NumSlots  uint16
	FreeStart uint16
	FreeEnd   uint16
	Data      [PageSize]byte
}

const (
	SlottedPageHeaderSize = 12 // PageID(8) + NumSlots(2) + FreeStart(2)
	SlotSize              = 4  // Offset(2) + Length(2)
)

// NewSlottedPage creates a new slotted page
func NewSlottedPage(pageID uint64) *SlottedPage {
	sp := &SlottedPage{
		PageID:    pageID,
		NumSlots:  0,
		FreeStart: SlottedPageHeaderSize,
		FreeEnd:   PageSize,
	}
	sp.writeHeader()
	return sp
}

// writeHeader writes the page header
func (sp *SlottedPage) writeHeader() {
	binary.LittleEndian.PutUint64(sp.Data[0:8], sp.PageID)
	binary.LittleEndian.PutUint16(sp.Data[8:10], sp.NumSlots)
	binary.LittleEndian.PutUint16(sp.Data[10:12], sp.FreeStart)
}

// LoadSlottedPage loads a slotted page from raw data
func LoadSlottedPage(data [PageSize]byte) *SlottedPage {
	sp := &SlottedPage{
		Data: data,
	}
	sp.PageID = binary.LittleEndian.Uint64(data[0:8])
	sp.NumSlots = binary.LittleEndian.Uint16(data[8:10])
	sp.FreeStart = binary.LittleEndian.Uint16(data[10:12])

	// Calculate free end
	sp.FreeEnd = PageSize
	for i := uint16(0); i < sp.NumSlots; i++ {
		slotOffset := SlottedPageHeaderSize + (i * SlotSize)
		offset := binary.LittleEndian.Uint16(sp.Data[slotOffset : slotOffset+2])
		if offset < sp.FreeEnd {
			sp.FreeEnd = offset
		}
	}

	return sp
}

// InsertRow inserts a row into the page
func (sp *SlottedPage) InsertRow(rowData []byte) (uint16, error) {
	rowLen := uint16(len(rowData))

	// Check if we have space
	spaceNeeded := rowLen + SlotSize
	freeSpace := sp.FreeEnd - sp.FreeStart

	if freeSpace < spaceNeeded {
		return 0, fmt.Errorf("not enough space in page")
	}

	// Insert row data at the end (growing backwards)
	sp.FreeEnd -= rowLen
	copy(sp.Data[sp.FreeEnd:], rowData)

	// Add slot entry
	slotOffset := sp.FreeStart
	binary.LittleEndian.PutUint16(sp.Data[slotOffset:], sp.FreeEnd) // Offset
	binary.LittleEndian.PutUint16(sp.Data[slotOffset+2:], rowLen)   // Length

	slotID := sp.NumSlots
	sp.NumSlots++
	sp.FreeStart += SlotSize

	// Update header
	sp.writeHeader()

	return slotID, nil
}

// GetRow retrieves a row by slot ID
func (sp *SlottedPage) GetRow(slotID uint16) ([]byte, error) {
	if slotID >= sp.NumSlots {
		return nil, fmt.Errorf("invalid slot ID: %d", slotID)
	}

	slotOffset := SlottedPageHeaderSize + (slotID * SlotSize)
	offset := binary.LittleEndian.Uint16(sp.Data[slotOffset : slotOffset+2])
	length := binary.LittleEndian.Uint16(sp.Data[slotOffset+2 : slotOffset+4])

	if offset == 0 && length == 0 {
		return nil, fmt.Errorf("row has been deleted")
	}

	rowData := make([]byte, length)
	copy(rowData, sp.Data[offset:offset+length])

	return rowData, nil
}

// GetAllRows returns all rows in the page
func (sp *SlottedPage) GetAllRows() [][]byte {
	rows := make([][]byte, 0, sp.NumSlots)

	for i := uint16(0); i < sp.NumSlots; i++ {
		rowData, err := sp.GetRow(i)
		if err == nil {
			rows = append(rows, rowData)
		}
	}

	return rows
}

// IsFull checks if the page can fit more data
func (sp *SlottedPage) IsFull(dataSize uint16) bool {
	spaceNeeded := dataSize + SlotSize
	freeSpace := sp.FreeEnd - sp.FreeStart
	return freeSpace < spaceNeeded
}
