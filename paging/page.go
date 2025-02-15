package paging

import (
	"boro-db/heap"
	"boro-db/utils/checksums"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"
)

/*
PagefileBlock inside heap file
┌──────────────────────────────────────────────────────────────┐
| checkSum (4 bytes)                                           |
|──────────────────────────────────────────────────────────────|
| ......                                                       |
|-------------------------- System Page Size (4096) -----------|
|                                                              |
└──────────────────────────────────────────────────────────────┘

A single pageFileBlock is composed of multiple pages
A page is a 4kb or whatever the OS prescribes as the smallest atomic unit of writable data
The unit can be larger as well so consider that as well

A pageBlock is a collection of such pages.
A buffer pool in itself cannot do a durable write to disk.
So a PageBlock Write has to be broken down into multiple page writes to make it actually durable
- So all modification will be translated into a physiological change by the PageBlock write function
-- A write function in PageBlock will capture the information where which mods were done and return the
pages interacted with for the mods.
-- A standard write from PageBuffer system in the WAL will look <pageNumber , pageOffset , lengthOfDatauffer , newDataBuffer>
-- This can now be translated by the Record manager which is deploying the write command into <pageNumber , pageOffset , lengthOfData , operationCode >
-- This will allow us to interact with only that page when interacting with the changes
-- a default implementation from the WAL can provide direct physical changes done to the pages for sake of simplicity i.e physical logging
*/
const pageBufferBlockByteOffset = 8

var ErrOutOfBounds = fmt.Errorf("out of bounds")

type PageFileBlock struct {

	// buffer contains entire page data use getter and setters
	// to change the data
	pageNumber uint64
	dirty      bool
	buffer     []byte
	crcMatch   bool
	mutex      sync.Mutex
}

func PageFileBlockBufferMaxSize(pageSizeByte uint32) uint32 {
	return pageSizeByte - pageBufferBlockByteOffset
}

func (pfb *PageFileBlock) Size() int {
	return (len(pfb.buffer))
}

func (pfb *PageFileBlock) GetPageMetaData() []byte {

	return pfb.buffer[0:pageBufferBlockByteOffset]
}

func (pfb *PageFileBlock) GetCheckSumBuffer() []byte {
	return pfb.buffer[0:4]
}

func (pfb *PageFileBlock) GetPostCRCBuffer() []byte {
	return pfb.buffer[4:]
}

func (pfb *PageFileBlock) GetPageBuffer() []byte {
	return pfb.buffer[pageBufferBlockByteOffset:]
}

func (pfb *PageFileBlock) CheckCRCMatch() bool {

	crc := crc32.ChecksumIEEE(pfb.GetPostCRCBuffer())

	crcMatch := crc == binary.BigEndian.Uint32(pfb.GetCheckSumBuffer())
	pfb.crcMatch = crcMatch
	return crcMatch
}

func (pfb *PageFileBlock) SetPageBuffer(offset int, buffer []byte, option *heap.FileOptions) error {

	if offset > len(pfb.buffer)-pageBufferBlockByteOffset || len(buffer) > len(pfb.buffer)-pageBufferBlockByteOffset {
		return ErrOutOfBounds
	}

	dataRegion := pfb.buffer[pageBufferBlockByteOffset+offset : pageBufferBlockByteOffset+offset+len(buffer)]
	copy(dataRegion, buffer)

	pfb.dirty = true
	return nil
}

func (pfb *PageFileBlock) Serialize() []byte {

	if pfb.dirty {
		checksums.CalculateCRC(pfb.GetCheckSumBuffer(), pfb.GetPostCRCBuffer())
	}
	return pfb.buffer
}
