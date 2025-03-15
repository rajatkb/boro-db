package freelist

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBitMapFreeList(t *testing.T) {
	bitmap := make([]byte, 6)
	totalAddress := 6 * 8
	freelist := NewBitmapFreeList(bitmap, 0, uint64(totalAddress)-1)
	pages, err := freelist.GetPages(10)

	assert.Nil(t, err)
	assert.Equal(t, 10, len(pages))
	assert.Equal(t, pages, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	assert.Equal(t, uint8(255), bitmap[0])
	assert.Equal(t, uint8(3), bitmap[1])
	assert.True(t, freelist.FreePageAvailable())

	freelist.ReleasePages([]uint64{10}) // no-op

	assert.Equal(t, uint8(255), bitmap[0])
	assert.Equal(t, uint8(3), bitmap[1])

	freelist.ReleasePages([]uint64{0, 1, 2})
	assert.Equal(t, uint8(0xF8), bitmap[0])

	pages, err = freelist.GetPages(1)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(pages))
	assert.Equal(t, uint64(2), pages[0])

	pages, err = freelist.GetPages(50)

	assert.Nil(t, err)
	assert.Equal(t, 39, len(pages))
	assert.False(t, freelist.FreePageAvailable())

}
