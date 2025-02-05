package paging

/**
- We have page key and then we have offset in the file we are working with
  we are keeping page offset = page key * page size
  there is a maximum size till which our page can grow

  Future scope
  - Offset cannot be a function of page key
  - we will require a sepparate page metadata whose responsibility is to only page number -> offset mapping
  - why is this needed ? because page compression.
  - pages can be heavily fragmented. compressing them can help save some of the storage requirements
  -
**/

// type SimpleLRU[S any, T TranslatePage[S]] struct {
// 	// we are using syncMap since we dont know if a page cache per goroutine
// 	// is a reasonable design choice or not
// 	// we are allowing for async write / read onto the pages through multiple threads
// 	pageCache            *cache.LRUCache[uint64, *Page[S, T]]
// 	fileop               file.FileOperation
// 	logger               *log.Logger
// 	createPageTranslator func([]byte) TranslatePage[S]
// 	options              PagingOptions
// }

// func (slru *SimpleLRU[S, T]) Read(key uint64, onRead func(*Page[S, T], error)) {

// 	value, ok := slru.pageCache.Get(key)
// 	if ok {
// 		onRead(value, nil)
// 		return
// 	}

// 	// // TODO : move this to a background thread allowing for batched preads
// 	// freshly minted disk page
// 	slru.fileop.Read(key, func(pageBlock []byte, err error) {
// 		page := &Page[S, T]{
// 			Key:      key,
// 			Dirty:    false,
// 			lock:     &sync.RWMutex{},
// 			PageData: slru.createPageTranslator(pageBlock),
// 		}
// 		slru.pageCache.Put(key, page)
// 		onRead(page, err)
// 	})
// }

// func (slru *SimpleLRU[S, T]) Write(page *Page[S, T], onWrite func(*Page[S, T], error)) {

// 	page.Dirty = true
// 	slru.pageCache.Put(page.Key, page)
// 	onWrite(page, nil)
// }

// func (slru *SimpleLRU[S, T]) Flush() error {

// 	var externalErr error
// 	slru.pageCache.Range(func(key uint64, p *Page[S, T]) bool {
// 		if p.Dirty {
// 			value := p.PageData.Flush()
// 			// offset not specified since pager is not aware of of page -> offset mapping
// 			if err := slru.fileop.WriteSync(key, value); err != nil {
// 				slru.logger.Error().Err(err).Msgf("page cache compaction and flush failed. error flushing page %d", key)
// 				externalErr = err
// 				return false
// 			}
// 		}
// 		return true
// 	})

// 	return externalErr
// }

// func createSimpleLRU[S any, T TranslatePage[S]](createPageTranslator func([]byte) TranslatePage[S], fileop file.FileOperation, logger *log.Logger, options PagingOptions) SwapStrategy[S, T] {

// 	slru := &SimpleLRU[S, T]{
// 		pageCache:            cache.NewLRUCache[uint64, *Page[S, T]]((options.PageCacheSize), true),
// 		fileop:               fileop,
// 		logger:               logger,
// 		createPageTranslator: createPageTranslator,
// 		options:              options,
// 	}

// 	go func() {
// 		ticker := time.NewTicker(time.Duration(options.FlushIntervalMs) * time.Millisecond)
// 		for {
// 			slru.Flush()
// 			<-ticker.C
// 		}
// 	}()

// 	return slru
// }
