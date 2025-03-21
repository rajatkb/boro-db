package paging

import (
	"boro-db/heap"
	"boro-db/utils/cache"
	"fmt"
	"sync"
	"time"

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
*/
type PageSystemOption struct {
	heap.HeapFileOptions
	PageBufferCacheSize          int
	BufferPoolEvictionIntervalms int
	BufferPoolFlushIntervalms    int
	EnablePageMeta               bool
}

type PageSystem interface {

	/*
		- read the pageBlock from in memory cache
		- if in memory cache is not available then read from disk

	*/
	ReadPage(pageNumber uint64, onRead func(*Page, error))

	/*
		- FlushPageBlock on the memory copy of the data
		- if memory copy is not available , should not be the case most of the times , read in memory and edit
		- the external system using the Page system is responsible for invoking WAL to ensure the data is journaled
		- if the external system has journaled the write then there should be no problems
		- bufferPool gives zero garauntees on writes on disk. an acceptance of write here does not mean the data is persisted instantly
		- the bufferpool will batch bunch of writes together and attempt to unload the writes in a single batched request
		-
	*/
	FlushPageBlock(pfb *Page, onWrite func(error))
	/*
		- Flush all the pages in the buffer pool force flush
	*/
	Flush() error
}

type pageSystem struct {
	heapfs  heap.HeapFile
	options PageSystemOption
	cache   cache.Cache[uint64, *Page]
}

func (ps *pageSystem) ReadPage(pageNumber uint64, onRead func(*Page, error)) {

	pfb, ok := ps.cache.Get(pageNumber)

	if ok {
		onRead(pfb, nil)
		return
	}

	// TODO :
	// create a pooled objects of PageFileBlock
	// hold the buffer memory with in
	// expand the array judiciously not at once
	// use the PageFileBlock to create a free size list which always points to first free block
	pfb = &Page{
		pageNumber:      pageNumber,
		buffer:          make([]byte, ps.options.PageSizeByte),
		pageMetaEnabled: ps.options.EnablePageMeta,
	}
	ps.heapfs.Read(pageNumber, pfb.buffer, func(err error) {
		if err != nil {
			onRead(nil, err)
		}
		ps.cache.Put(pageNumber, pfb)
		onRead(pfb, nil)
	})
}

func (ps *pageSystem) FlushPageBlock(pfb *Page, onWrite func(error)) {
	pfb.mutex.RLock()
	defer pfb.mutex.RUnlock()
	ps.heapfs.Write(pfb.pageNumber, pfb.serialize(), func(err error) {
		if err != nil {
			onWrite(err)
		}
		onWrite(nil)
	})
}

func (ps *pageSystem) Flush() error {
	var flushWg sync.WaitGroup
	ps.cache.Range(func(u uint64, pfb *Page) bool {
		pfb.mutex.RLock()
		if pfb.dirty {
			// if dirty , do not wait for the write op to complete
			// increment semaphor
			// start page write op in disk
			// don't unlock the mutex until the write is complete (its a read lock so all reads are still allowed)
			// writes would be blocked (internally dity is set to false and writes turn dirty to true)
			// we unlock only once write is complete

			flushWg.Add(1)
			ps.heapfs.Write(pfb.pageNumber, pfb.serialize(), func(err error) {
				if err != nil {
					log.Error().Err(err).Msg(fmt.Sprintf("error flushing page : %d", pfb.pageNumber))
				} else {
					pfb.dirty = false
				}
				pfb.mutex.RUnlock()
				flushWg.Done()
			})
			return true
		}
		pfb.mutex.RUnlock()
		return true
	})
	flushWg.Wait()
	return nil
}

/*
Caching on top of heap file
heap files are raw file and buffer space
PageSystem gives heapfile page block structure
*/
func NewPageSystem(logger log.Logger, heapfs heap.HeapFile, options PageSystemOption) (PageSystem, error) {

	cache := cache.NewLRUCache[uint64, *Page](options.PageBufferCacheSize)
	ps := &pageSystem{
		heapfs: heapfs,

		options: options,
		cache:   cache,
	}
	go func() {
		evictionTicker := time.NewTicker(time.Millisecond * time.Duration(options.BufferPoolEvictionIntervalms))
		flushTicker := time.NewTicker(time.Millisecond * time.Duration(options.BufferPoolFlushIntervalms))
		var wg sync.WaitGroup
		lastEvictionTickerTime := time.Now()
		for {
			select {
			case <-flushTicker.C:
				ps.Flush()
			case <-evictionTicker.C:
				now := time.Now()

				if now.Sub(lastEvictionTickerTime).Milliseconds() > int64(options.BufferPoolEvictionIntervalms) {
					// this means the last operation took much longer than it should have
					// we crossed a ticker instance its reasonable to actually skip to the next tick
					lastEvictionTickerTime = now
					continue
				}

				cache.Compact(func(u uint64, pfb *Page) bool {
					pfb.mutex.RLock()
					if pfb.dirty {
						// if dirty , do not wait for the write op to complete
						// incremental semaphor
						// start page write op in disk
						// don't unlock the mutex until the write is complete (its a read lock so all reads are still allowed)
						// writes would be blocked (internally dity is set to false and writes turn dirty to true)
						// we unlock only once write is complete
						wg.Add(1)
						heapfs.Write(pfb.pageNumber, pfb.serialize(), func(err error) {
							if err != nil {
								log.Error().Err(err).Msg(fmt.Sprintf("error flushing page : %d", pfb.pageNumber))
							}
							pfb.dirty = false
							pfb.mutex.RUnlock()
							wg.Done()
						})
						return false
					}
					pfb.mutex.RUnlock()
					return true
				})

				// wait for all the files to get flushed before moving to next tick
				wg.Wait()
			}
		}
	}()

	return ps, nil
}
