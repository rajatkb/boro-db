package freelist

type FreeList interface {
	GetPages(count uint64) ([]uint64, error)
	ReleasePages(pages []uint64) error
	Serialize([]byte)
}

type BitmapFreeList struct {
	bitmap []byte
}
