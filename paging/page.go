package paging

import (
	"boro-db/utils/checksums"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"
)

/*
PagefileBlock inside heap file
┌──────────────────────────────────────────────────────────────┐
| checkSum (4 bytes) | LSN (4byte)                             |
|──────────────────────────────────────────────────────────────|
| ......                                                       |
|-------------------------- System Page Size (4096) -----------|
└──────────────────────────────────────────────────────────────┘
*/
const pageBufferBlockByteOffset = 8

var ErrOutOfBounds = fmt.Errorf("out of bounds")

type Page struct {

	// buffer contains entire page data use getter and setters
	pageNumber uint64
	dirty      bool
	buffer     []byte
	crcMatch   bool
	// TODO : remove the mutex lock , and try a CAS operation + Scheduler
	mutex           sync.RWMutex
	currentLSN      uint32
	pageMetaEnabled bool
}

func (pfb *Page) Size() int {

	if pfb.pageMetaEnabled {
		return len(pfb.buffer) - pageBufferBlockByteOffset
	}

	return (len(pfb.buffer))
}

func (pfb *Page) GetCheckSumBuffer() []byte {

	if pfb.pageMetaEnabled {
		return pfb.buffer[0:4]
	}

	return nil
}

func (pfb *Page) GetPostCRCBuffer() []byte {
	if pfb.pageMetaEnabled {
		return pfb.buffer[4:]
	}
	return nil
}

func (pfb *Page) GetLSNBUffer() []byte {

	if pfb.pageMetaEnabled {
		return pfb.buffer[4:8]
	}
	return nil
}

func (pfb *Page) CheckCRCMatch() bool {

	if !pfb.pageMetaEnabled {
		return true
	}

	crc := crc32.ChecksumIEEE(pfb.GetPostCRCBuffer())
	crcMatch := crc == binary.BigEndian.Uint32(pfb.GetCheckSumBuffer())

	pfb.crcMatch = crcMatch

	return pfb.crcMatch
}

func (pfb *Page) SetPageBuffer(offset int, buffer []byte, currentLSN uint32) error {

	pfb.mutex.Lock()
	defer pfb.mutex.Unlock()

	dataRegion := pfb.buffer
	if pfb.pageMetaEnabled {
		if offset > len(pfb.buffer)-pageBufferBlockByteOffset || len(buffer) > len(pfb.buffer)-pageBufferBlockByteOffset {
			return ErrOutOfBounds
		}
		dataRegion = pfb.buffer[pageBufferBlockByteOffset+offset : pageBufferBlockByteOffset+offset+len(buffer)]
	}

	if offset > len(pfb.buffer) || len(buffer) > len(pfb.buffer) {
		return ErrOutOfBounds
	}

	copy(dataRegion, buffer)
	pfb.currentLSN = currentLSN
	pfb.dirty = true

	return nil
}

func (pfb *Page) GetPageBuffer(onRead func([]byte)) {
	pfb.mutex.RLock()
	defer pfb.mutex.RUnlock()
	if pfb.pageMetaEnabled {
		onRead(pfb.buffer[pageBufferBlockByteOffset:])
	} else {
		onRead(pfb.buffer)
	}
}

func (pfb *Page) serialize() []byte {
	pfb.mutex.RLock()
	defer pfb.mutex.RUnlock()

	if pfb.dirty && pfb.pageMetaEnabled {
		checksums.CalculateCRC(pfb.GetCheckSumBuffer(), pfb.GetPostCRCBuffer())
		binary.BigEndian.PutUint32(pfb.GetLSNBUffer(), pfb.currentLSN)
	}

	return pfb.buffer
}
