package file

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsingPageRefData(t *testing.T) {

	plb := pageLocationBits{
		freeBytesBits: uint64(math.Ceil(math.Log2(float64(MIN_PAGE_SIZE)))),
		offsetBits:    uint64(math.Ceil(math.Log2(float64(MAX_HEAP_FILE_SIZE / MIN_PAGE_SIZE)))),
	}

	plb.fileIDBits = 64 - plb.offsetBits - plb.freeBytesBits

	assert.Equal(t, uint64(64), plb.fileIDBits+plb.offsetBits+plb.freeBytesBits)
	assert.Equal(t, plb, pageLocationBits{
		freeBytesBits: 12,
		offsetBits:    20,
		fileIDBits:    32,
	})

	buffer := make([]byte, 8)

	expected := &pageRefData{
		fileID:    1,
		offset:    2,
		freeBytes: 3,
	}

	putPageLocationBuffer(buffer, expected, &plb)
	assert.Equal(t, expected, extractPageLocation(buffer, &plb))

}
