package paging

import (
	"fmt"
)

const PAGE_CACHE_SIZE = 1000 //  1000 pages * 4kb = 4mb

var (
	ErrInvalidOffset error = fmt.Errorf("invalid offset , exceeds page size")
	ErrPayloaToLarge error = fmt.Errorf("payload too large")
)

type PageStrategy int

const (
	SimpleLRUStrategy PageStrategy = iota
)

type SwapStrategy[S any, T TranslatePage[S]] interface {
	// Reads a page from disk and returns
	// internally this may or may not hit the page cache
	// if the page is not in the cache, it will be read from the disk
	Read(key uint64, onRead func(*Page[S, T], error))

	// Write a binary back into the disk
	// internally this may or may not flush this data
	// overwrites the existing data. Pager is not responsible for the state of data inside a page
	// but only the page alone
	// if the page is written into memory cache it will be marked dirty until flushed
	Write(page *Page[S, T], onWrite func(*Page[S, T], error))

	// to flush the buffers asap
	// useful when closing the application
	Flush() error
}

type TranslatePage[T any] interface {
	GetInstance() T
	Flush() []byte
}

type PagingOptions struct {
	PageStrategy    PageStrategy
	FlushIntervalMs int
	PageSize        uint64
	PageCacheSize   int
}

// func NewPagingStrategy[S any, T TranslatePage[S]](createPageTranslator func([]byte) TranslatePage[S], fileop file.FileOperation, logger *log.Logger, options PagingOptions) SwapStrategy[S, T] {

// 	return createSimpleLRU[S, T](createPageTranslator, fileop, logger, options)
// }
