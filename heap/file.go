package heap

const MIN_PAGE_SIZE = uint32(4096)                        // 4kb
const MAX_HEAP_FILE_SIZE = uint32(2 * 1024 * 1024 * 1024) // 1GB

type FileOptions struct {
	PageSizeByte     uint32 // size of one page block in bytes
	FileDirectory    string // file directory where the heap files are located
	HeapFileSizeByte uint32 // size of heap file inclusive of the metadata. count of page = heapfileSizeByte / pageSizeByte - 1
	RequireFreeList  bool   // use only if doing random allocation and revokation
}

type HeapFile interface {
	// TrimHead(firstPageNumber uint64) error
	// Trims the address space to the last page number
	TrimHead(count uint64) error
	// ExtendBy adds the number of pages to the address
	ExtendBy(pageCount int) error
	Read(pageNumber uint64, buffer []byte, onRead func(error))
	Write(pageNumber uint64, buffer []byte, onWrite func(error))
	ValidAddressRange() [2]uint64
	Malloc(count uint64) ([]uint64, error)
	Free(pageNumbers []uint64) error
	FreePagesAvailable() uint64
}
