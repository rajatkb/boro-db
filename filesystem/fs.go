package filesystem

import "boro-db/paging"

/*
Filesystem uses the Paging System + Heap to
provide a usable memory + durable persistent
memory model.

Any data structure using the filesystem can be assured
that writes going through this will be durable to disk.

Free Size and Garbage Collection
*/
type FileSystem interface {

	// Seek and read the given page in page number
	Read(pageNumber uint64, onRead func(*paging.Page, error))

	// Grab contiguous or non contiguous pages (preferably contiguous)
	Alloc(count uint64) ([]*paging.Page, error)

	// Mark the pages free for future usage
	DeAlloc(pages []*paging.Page) error
}

type localfilesystem struct {
}

func (lfs *localfilesystem) Read(pageNumber uint64, onRead func(*paging.Page, error)) {

}

func (lfs *localfilesystem) Alloc(count uint64) ([]*paging.Page, error) {
	return nil, nil
}

func (lfs *localfilesystem) DeAlloc(pages []*paging.Page) error {
	return nil
}

func NewFileSystem() FileSystem {

	return &localfilesystem{}
}
