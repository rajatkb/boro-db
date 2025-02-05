package file

import (
	"fmt"
)

const MIN_PAGE_SIZE = uint32(4096)                        // 4kb
const MAX_HEAP_FILE_SIZE = uint32(2 * 1024 * 1024 * 1024) // 1GB

type FileOptions struct {
	ReadBatchSize    int
	WriteBatchSize   int
	PageSizeByte     uint32
	FileDirectory    string
	HeapFileSizeByte uint32
}

type PageMeta struct {
	RecordIndex   uint64
	RecordsOffset uint64
	FileID        uint64
	PageFreeBytes uint64
	FileOffset    uint64
}

type FileOperation interface {

	// read a page from the heap file
	Read(pageID uint64, readCompleteCallback func(*PageFileBlock, error))
	// write a page to the heap file
	Write(pageID uint64, data *PageFileBlock, writeCompleteCallback func(error))

	// add more pages to the heap file
	// file manager can decide to add more pages for byte alignment purposes
	Allocate(pageCount int) error

	// Next page in the list
	Next(pageID uint64) (uint64, error)
	// // Remove page in heap file
	// Free(pageID uint64)

	// // Next free page in the list
	// NextFree() (uint64, error)

	// // BestFitFreePage returns best fit page for given size
	// BestFitFreePage(pageSize uint32) (uint64, error)

	// BiggestFreePage() (uint64, error)

	GetPageCount() uint64

	GetPageMeta(pageID uint64) (*PageMeta, error)

	Close()
}

var ErrEOF = fmt.Errorf("end of file")
