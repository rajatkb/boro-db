package main

import (
	"boro-db/filesystem"
	"boro-db/heap"
	"boro-db/logging"
	"boro-db/paging"
)

func main() {
	logger := logging.CreateDebugLogger()

	heapFileOptions := heap.HeapFileOptions{
		PageSizeByte:        4096,
		FileDirectory:       "./test",
		MaxHeapFileSizeByte: 1024 * 1024 * 1024, // 1GB
	}
	fs, err := filesystem.NewFileSystem(*logger, &filesystem.FileSystemOptions{
		HeapFileOptions: heapFileOptions,
		PageSystemOption: paging.PageSystemOption{
			HeapFileOptions:              heapFileOptions,
			PageBufferCacheSize:          1024 * 1024,
			BufferPoolEvictionIntervalms: 10000,
			BufferPoolFlushIntervalms:    1000,
			EnablePageMeta:               false,
		},
		ExtendAddressSpaceByPageCount: int(heapFileOptions.MaxHeapFileSizeByte) / int(heapFileOptions.PageSizeByte),
	})

	if err != nil {
		logger.Error().Err(err).Msg("failed to create filesystem")
		return
	}

	pages, err := fs.Malloc(1)

	if err != nil {
		logger.Error().Err(err).Msg("failed to malloc pages")
		return
	}

	fs.Write(pages[0], func(p *paging.Page, err error) {
		p.SetPageBuffer(0, []byte("hello world"), 0)
	})

	fs.Read(pages[0], func(p *paging.Page, err error) {
		p.GetPageBuffer(func(b []byte) {
			logger.Info().Msg(string(b))
		})
	})

	fs.Flush()
}
