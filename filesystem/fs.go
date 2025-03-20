package filesystem

import (
	"boro-db/heap"
	"boro-db/paging"
	"errors"

	"github.com/phuslu/log"
)

/*
Filesystem uses the Paging System + Heap to
provide a usable memory + durable persistent
memory model.

Any data structure using the filesystem can be assured
that writes going through this will be durable to disk.

Free Size and Garbage Collection
*/
type FileSystem interface {
	Write(pageNumber uint64, doWrite func(*paging.Page, error))
	// Seek and read the given page in page number
	Read(pageNumber uint64, onRead func(*paging.Page, error))

	// Grab contiguous or non contiguous pages (preferably contiguous)
	Malloc(count uint64) ([]uint64, error)

	// Mark the pages free for future usage
	Free(pages []uint64) error
}

type FileSystemOptions struct {
	heap.HeapFileOptions
	paging.PageSystemOption
	ExtendAddressSpaceBy int
}

type localfilesystem struct {
	options *FileSystemOptions
	heap    heap.HeapFile
	paging  paging.PageSystem
	logger  log.Logger
}

/*
Check if address is valid allocated region address
read the page from paging system (this can be recently allocated item as well without ever being writtenor not present in memory)
call doWrite on that page
*/
func (lfs *localfilesystem) Write(pageNumber uint64, doWrite func(*paging.Page, error)) {
	if !lfs.heap.IsPageFree(pageNumber) {
		lfs.paging.ReadPage(pageNumber, func(page *paging.Page, err error) {
			if err != nil {
				doWrite(nil, err)
			} else {
				doWrite(page, nil)
			}
		})
	} else {
		doWrite(nil, errors.New("page is not allocated"))
	}
}

/*
Seek and read the given page in page number
Only use BufferPool for this purpose
*/
func (lfs *localfilesystem) Read(pageNumber uint64, onRead func(*paging.Page, error)) {
	if !lfs.heap.IsPageFree(pageNumber) {
		lfs.paging.ReadPage(pageNumber, func(page *paging.Page, err error) {
			if err != nil {
				onRead(nil, err)
			} else {
				onRead(page, nil)
			}
		})
	}
}

/*
Similar to malloc in C or make in go
Provides memory addresses for Pages to work with
- If address space has enough free pages then allocate + read + return
- If address space does not have enough free pages then allocate + extend address space + allocate remaining + read + return
*/
func (lfs *localfilesystem) Malloc(count uint64) ([]uint64, error) {

	pages, err := lfs.heap.Malloc(count)
	if err != nil {
		lfs.logger.Error().Err(err).Msg("error allocating pages")
		return nil, err
	}

	if len(pages) != int(count) {
		lfs.heap.ExtendBy(int(lfs.options.ExtendAddressSpaceBy))
		pg, err := lfs.heap.Malloc(count - uint64(len(pages)))
		if err != nil {
			lfs.logger.Error().Err(err).Msg("error allocating pages")
			err := lfs.heap.Free(pages)
			lfs.logger.Error().Err(err).Msg("error freeing pages when trying to fix allocation")
			return nil, err
		}
		return append(pages, pg...), nil
	}

	return pages, nil
}

/*
Similar to free in C
Frees the page numbers provided. This means these pages can now be used while allocation
*/
func (lfs *localfilesystem) Free(pages []uint64) error {

	// TODO : remove these pages from cache as well in case they are still present
	lfs.heap.Free(pages)

	return nil
}

func NewFileSystem(logger log.Logger, options *FileSystemOptions) (FileSystem, error) {

	heap, err := heap.NewHeap(logger, &options.HeapFileOptions)

	if err != nil {
		logger.Error().Err(err).Msg("error creating heap")
		return nil, err
	}

	paging, err := paging.NewPageSystem(logger, heap, options.PageSystemOption)

	if err != nil {
		logger.Error().Err(err).Msg("error creating paging")
		return nil, err
	}

	return &localfilesystem{
		heap:    heap,
		paging:  paging,
		options: options,
		logger:  logger,
	}, nil
}
