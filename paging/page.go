package paging

import "sync"

type Page[S any, T TranslatePage[S]] struct {
	Key      uint64
	PageData TranslatePage[S]
	// status if the page is commited to the disk
	Dirty bool
	lock  *sync.RWMutex

	// metadata
	pageId     uint64 // page id for the current page
	pageSize   uint64 // size of page post reading (will be synced once the page is written)
	diskOffset uint64 // offset of the page in the file

}
