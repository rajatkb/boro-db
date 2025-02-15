package heap

const MIN_PAGE_SIZE = uint32(4096)                        // 4kb
const MAX_HEAP_FILE_SIZE = uint32(2 * 1024 * 1024 * 1024) // 1GB

type FileOptions struct {
	PageSizeByte     uint32 // size of one page block in bytes
	FileDirectory    string // file directory where the heap files are located
	HeapFileSizeByte uint32 // size of heap file inclusive of the metadata. count of page = heapfileSizeByte / pageSizeByte - 1
}

type HeapFile interface {
	Truncate(lastPageNumber uint64) error
	AllocatePage(pageCount int) error
	Read(pageNumber uint64, buffer []byte, onRead func(error))
	Write(pageNumber uint64, buffer []byte, onWrite func(error))
	MaxAddressablePage() uint64
}
