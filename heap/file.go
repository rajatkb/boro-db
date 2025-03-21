package heap

const MIN_PAGE_SIZE = uint32(4096)                        // 4kb
const MAX_HEAP_FILE_SIZE = uint32(2 * 1024 * 1024 * 1024) // 1GB

type HeapFileOptions struct {
	PageSizeByte        uint32 // size of one page block in bytes
	FileDirectory       string // file directory where the heap files are located
	MaxHeapFileSizeByte uint32 // size of heap file inclusive of the metadata. count of page = heapfileSizeByte / pageSizeByte - 1
}

type HeapFile interface {

	// Deletes the heap files based on the new address space start
	// any heap file with address space lesser than by the new address
	// space will be deleted
	TrimTailHeapFiles(count uint64) error

	// Trims and deletes heap files based on new last address in address space
	TrimHead(count uint64) error
	// ExtendBy adds the number of pages to the address
	ExtendBy(pageCount int) error
	Read(pageNumber uint64, buffer []byte, onRead func(error))
	Write(pageNumber uint64, buffer []byte, onWrite func(error))
	ValidAddressRange() [2]uint64

	// Part of free space management system
	// if heap files are used as immutable log file ignore this
	// if heap files are used as mutable address space use this
	Malloc(count uint64) ([]uint64, error)
	Free(pageNumbers []uint64) error
	FreePagesAvailable() uint64
	// Checks if given page is free or not. if it out of range return false
	// use it always before Read / Write if you care about allocation
	IsPageFree(pageNumber uint64) bool
}
