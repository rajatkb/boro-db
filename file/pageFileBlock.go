package file

import (
	"crypto/md5"
	"encoding/binary"
	"math"
)

/*
Pagefile inside heap file
┌──────────────────────────────────────────────────────────────┐
| checkSum (4 bytes) | LSN 4 byte                              |
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
	option *FileOptions
}

func PageBufferMaxSize(pageSizeByte uint64) uint64 {
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

func (pfb *PageFileBlock) SetPageBuffer(buffer []byte, freeSize uint32) {
	dataRegion := pfb.buffer[pageBufferBlockByteOffset:]
	copy(dataRegion, buffer)

	freeSize = uint32(math.Min(float64(PageBufferMaxSize(uint64(pfb.option.PageSizeByte))), float64(freeSize)))

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

func putEmptyHeapPage(buffer []byte, pageSizeByte uint64) {
	// setting size
	pfb := PageFileBlock{
		buffer: buffer,
	}

	binary.BigEndian.PutUint64(pfb.getFreeBytesBuffer(), PageBufferMaxSize(pageSizeByte))

	calculateCRC(pfb.getCheckSumBuffer(), pfb.GetPageBuffer())
}

func readHeapPage(buffer []byte) *PageFileBlock {
	return &PageFileBlock{
		buffer: buffer,
	}
}
