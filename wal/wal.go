package wal

import (
	"boro-db/heap"
	"boro-db/paging"

	"github.com/phuslu/log"
)

type Wal struct {
	logger  log.Logger
	heap    heap.HeapFile
	page    paging.PageSystem
	options *WalOptions
}

type WalOptions struct {
	FileDirectory string
	SegmentSizes  uint32
}

func (w *Wal) Append(data []byte, onWrite func(uint64, error)) {

}

func NewWal(logger log.Logger, options *WalOptions) (*Wal, error) {
	heapOptions := &heap.HeapFileOptions{
		PageSizeByte:        4096,
		FileDirectory:       options.FileDirectory,
		MaxHeapFileSizeByte: options.SegmentSizes,
	}
	heapfs, err := heap.NewHeap(logger, heapOptions)

	if err != nil {
		logger.Error().Err(err).Msg("error creating heap")
		return nil, err
	}

	pagesys, err := paging.NewPageSystem(logger, heapfs, paging.PageSystemOption{
		HeapFileOptions:              *heapOptions,
		PageBufferCacheSize:          int(options.SegmentSizes) / int(heapOptions.PageSizeByte),
		BufferPoolEvictionIntervalms: 100,
		BufferPoolFlushIntervalms:    100,
	})

	if err != nil {
		logger.Error().Err(err).Msg("error creating paging")
		return nil, err
	}

	return &Wal{
		logger:  logger,
		heap:    heapfs,
		page:    pagesys,
		options: options,
	}, nil
}
