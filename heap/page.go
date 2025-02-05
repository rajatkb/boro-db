package heap

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"math"
)

/*
Pagefile inside heap file
┌──────────────────────────────────────────────────────────────┐
| checkSum (4 bytes) | freebytes 4 byte                        |
|──────────────────────────────────────────────────────────────|
| ......                                                       |
└──────────────────────────────────────────────────────────────┘
*/
const pageBufferBlockByteOffset = 8

type PageFileBlock struct {

	// buffer contains entire page data use getter and setters
	// to change the data
	dirty  bool
	buffer []byte
}

func PageBufferMaxSize(pageSizeByte uint32) uint32 {
	return pageSizeByte - pageBufferBlockByteOffset
}

func (pfb *PageFileBlock) getPageMetaData() []byte {
	return pfb.buffer[0:pageBufferBlockByteOffset]
}

func (pfb *PageFileBlock) getCheckSumBuffer() []byte {
	return pfb.buffer[0:4]
}

func (pfb *PageFileBlock) getFreeBytesBuffer() []byte {
	return pfb.buffer[4:8]
}

func (pfb *PageFileBlock) GetPageBuffer() []byte {
	return pfb.buffer[pageBufferBlockByteOffset:]
}

func (pfb *PageFileBlock) SetPageBuffer(buffer []byte, freeSize uint32, option *FileOptions) {
	dataRegion := pfb.buffer[pageBufferBlockByteOffset:]
	copy(dataRegion, buffer)

	freeSize = uint32(math.Min(float64(PageBufferMaxSize((option.PageSizeByte))), float64(freeSize)))

	binary.BigEndian.PutUint32(pfb.getFreeBytesBuffer(), freeSize)
	pfb.dirty = true
}

func (pfb *PageFileBlock) Serialize() []byte {

	if pfb.dirty {
		chksum := md5.Sum(pfb.buffer[pageBufferBlockByteOffset:])
		copy(pfb.getPageMetaData(), chksum[:])
	}
	return pfb.buffer
}

func putEmptyHeapPage(buffer []byte, pageSizeByte uint32, count int) error {

	if len(buffer) < int(pageSizeByte)*count {
		return fmt.Errorf("buffer size is not enough")
	}
	pfb := PageFileBlock{}

	for i := 0; i < count; i++ {
		pfb.buffer = buffer[i*int(pageSizeByte) : (i+1)*int(pageSizeByte)]
		binary.BigEndian.PutUint32(pfb.getFreeBytesBuffer(), PageBufferMaxSize(pageSizeByte))
		calculateCRC(pfb.getCheckSumBuffer(), pfb.GetPageBuffer())
	}
	return nil
}

func readPageFileBlock(buffer []byte, pageSizeByte uint64) *PageFileBlock {

	pfb := &PageFileBlock{
		buffer: buffer,
	}

	return pfb
}
