package paging

import (
	"boro-db/heap"
	"boro-db/utils/cache"

	"github.com/phuslu/log"
)

/*
What is a buffer pool or paging system for us
- We have heap files which only expands or shrinks
- some implementations like WAL may be able to run cleanups on older heap / segment files
- but in the end all of this will have to be loaded in memory and operated on
- the page file system is a way to abstract the loading of page blocks and flushing same
- pageBlock = page + metadata
- page = 4kb which is a OS / Hardware standard. this is also what our WAL logs will posibly follow
- pageBlock = 4mb which is something our database can control.

Why do we need a pageBlock. Because database tries to increase page locality and prefers writing large amount of data in one go
This layer does two things
- Decouples from hardware pagesize and disk limitations
- Gives an in memory cache to work with for the heap read / writes
*/
type PageSystemOption struct {
	heap.FileOptions
	PageBlockSize               uint32
	PageBufferCacheSize         int
	MultiThreadedWritesDisabled bool
}

type PageSystem interface {
	AllocatePage(pageCount int) error
	ReadPageBlock(pageNumber uint64, onRead func(*PageFileBlock, error))

	/*
		- FlushPageBlock on the memory copy of the data
		- if memory copy is not available , should not be the case most of the times , read in memory and edit
		- the external system using the Page system is responsible for invoking WAL to ensure the data is journaled
		- if the external system has journaled the write then there should be no problems
		- bufferPool gives zero garauntees on writes on disk. an acceptance of write here does not mean the data is persisted instantly
		- the bufferpool will batch bunch of writes together and attempt to unload the writes in a single batched request
		-
	*/
	FlushPageBlock(pfb *PageFileBlock, onWrite func(error))
	MaxAddressablePage() uint64
	Flush() error
}

type pageSystem struct {
	heap    heap.HeapFile
	options PageSystemOption
	cache   cache.Cache[uint64, *PageFileBlock]
}

func (ps *pageSystem) AllocatePage(pageCount int) error {
	return ps.heap.AllocatePage(pageCount * int(ps.options.PageBlockSize))
}

func (ps *pageSystem) ReadPageBlock(pageNumber uint64, onRead func(*PageFileBlock, error)) {

	pfb, ok := ps.cache.Get(pageNumber)

	if ok {
		onRead(pfb, nil)
		return
	}

	pfb = &PageFileBlock{
		pageNumber: pageNumber,
		buffer:     make([]byte, ps.options.PageBlockSize*ps.options.PageSizeByte),
	}
	ps.heap.Read(pageNumber, pfb.buffer, func(err error) {
		if err != nil {
			onRead(nil, err)
		}

		onRead(pfb, nil)
		ps.cache.Put(pageNumber, pfb)
	})
}

func (ps *pageSystem) FlushPageBlock(pfb *PageFileBlock, onWrite func(error)) {

	ps.heap.Write(pfb.pageNumber, pfb.Serialize(), func(err error) {
		if err != nil {
			onWrite(err)
		}
		onWrite(nil)
	})
}

func (ps *pageSystem) MaxAddressablePage() uint64 {
	return ps.heap.MaxAddressablePage()
}

func (ps *pageSystem) Flush() error {
	return nil
}

func NewPageSystem(logger log.Logger, options PageSystemOption) (PageSystem, error) {
	heap, err := heap.NewHeap(logger, options.FileOptions)
	if err != nil {
		return nil, err
	}
	return &pageSystem{
		heap:    heap,
		options: options,
		cache:   cache.NewLRUCache[uint64, *PageFileBlock](options.PageBufferCacheSize),
	}, nil
}
