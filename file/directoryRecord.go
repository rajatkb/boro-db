package file

import (
	"encoding/binary"
	"math"
)

/*
Directory Record Page
┌──────────────────────────────────────────────────────────────┐
| ┌──────────────────────────────────────────────────────────┐ |
│ | crc 4 byte | LSN 4 byte                                  | |
| |──────────────────────────────────────────────────────────| |
│ | (fileID , offset , freebytes) 8 bytes                    | |
| |──────────────────────────────────────────────────────────| |
| | repeat ...                                               | |
| |──────────────────────────────────────────────────────────| |
| └──────────────────────────────────────────────────────────┘ |
└──────────────────────────────────────────────────────────────┘

freeBytes = log_2(Max_Page_Size) bits
offset = log_2(maxHeapSize / pageSize) bits
fileID = 64 - offset - freeBytes bits

*/

type pageRefData struct {
	fileID    uint64
	offset    uint64
	freeBytes uint64
}

type pageLocationBits struct {
	fileIDBits    uint64
	offsetBits    uint64
	freeBytesBits uint64
}

func extractPageLocation(buffer []byte, pageLocationBits *pageLocationBits) *pageRefData {

	value := binary.BigEndian.Uint64(buffer)

	// Create masks based on bit widths
	fileIDMask := uint64((1 << pageLocationBits.fileIDBits) - 1)
	offsetMask := uint64((1 << pageLocationBits.offsetBits) - 1)
	freeByteMask := uint64((1 << pageLocationBits.freeBytesBits) - 1)

	// Extract values using bit shifting and masking
	// freeBytes is in the least significant bits
	freeBytes := value & freeByteMask

	// offset is in the middle
	offset := (value >> pageLocationBits.freeBytesBits) & offsetMask

	// fileID is in the most significant bits
	fileID := (value >> (pageLocationBits.freeBytesBits + pageLocationBits.offsetBits)) & fileIDMask

	return &pageRefData{
		fileID:    fileID,
		offset:    offset,
		freeBytes: freeBytes,
	}
}

func putPageLocationBuffer(buffer []byte, pageRefData *pageRefData, pageLocationBits *pageLocationBits) {
	// Create masks based on bit widths

	value := (pageRefData.fileID << (pageLocationBits.freeBytesBits + pageLocationBits.offsetBits))
	value = value | (pageRefData.offset << pageLocationBits.freeBytesBits)
	value = value | pageRefData.freeBytes

	binary.BigEndian.PutUint64(buffer, value)

}

const pageFileRefLength = 8 //byte
const pageFileRefByteOffset = 8

type directoryRecord struct {
	dirty      bool
	buffer     []byte
	option     *FileOptions
	pageRefBit *pageLocationBits
}

func (dr *directoryRecord) readPageRef(index int) pageRefData {
	return *extractPageLocation(dr.buffer[pageFileRefByteOffset+index*pageFileRefLength:pageFileRefByteOffset+(index+1)*pageFileRefLength], dr.pageRefBit)
}

func (dr *directoryRecord) setPageRef(index int, pageRefData pageRefData) {
	putPageLocationBuffer(dr.buffer[pageFileRefByteOffset+index*pageFileRefLength:pageFileRefByteOffset+(index+1)*pageFileRefLength], &pageRefData, dr.pageRefBit)
	dr.dirty = true
}

func (dr *directoryRecord) Serialize() []byte {
	if dr.dirty {
		calculateCRC(dr.buffer[0:4], dr.buffer[8:])
	}
	return dr.buffer
}

func totalPageRefInSingleDirectoryRecord() int {
	return int((MIN_PAGE_SIZE - pageFileRefByteOffset) / pageFileRefLength)
}

// TODO : create error handling here
func readDirectoryRecordPage(buffer []byte, option *FileOptions) *directoryRecord {
	return &directoryRecord{
		buffer: buffer,
		option: option,
	}
}

// createNewDirectoryRecordPages , creates a buffer of multiple directory pages
// these
func createNewDirectoryRecordPages(startPageID uint64, endPageID uint64) []byte {
	totalPages := math.Ceil(float64(endPageID-startPageID) / float64(MIN_PAGE_SIZE))

	pageBufferCollection := make([]byte, int(totalPages)*int(MIN_PAGE_SIZE))

	plb := pageLocationBits{
		freeBytesBits: uint64(math.Ceil(math.Log2(float64(MIN_PAGE_SIZE)))),
		offsetBits:    uint64(math.Ceil(math.Log2(float64(MAX_HEAP_FILE_SIZE) / float64(MIN_PAGE_SIZE)))),
	}

	plb.fileIDBits = 64 - plb.offsetBits - plb.freeBytesBits

	for i := 0; i < int(totalPages); i++ {
		pageBuffer := pageBufferCollection[i*int(MIN_PAGE_SIZE) : (i+1)*int(MIN_PAGE_SIZE)]

		for j := 0; j < totalPageRefInSingleDirectoryRecord(); j++ {
			putPageLocationBuffer(pageBuffer[pageFileRefByteOffset+j*pageFileRefLength:pageFileRefByteOffset+(j+1)*pageFileRefLength], &pageRefData{
				fileID:    startPageID + uint64(i),
				offset:    uint64(j),
				freeBytes: 0,
			}, &plb)
		}
		calculateCRC(pageBuffer[0:4], pageBuffer[8:])
	}

	return pageBufferCollection
}
