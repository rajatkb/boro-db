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
	mutex      sync.RWMutex
	currentLSN uint32
}

func (pfb *Page) Size() int {
	return (len(pfb.buffer))
}

func (pfb *Page) GetCheckSumBuffer() []byte {
	return pfb.buffer[0:4]
}

func (pfb *Page) GetPostCRCBuffer() []byte {
	return pfb.buffer[4:]
}

func (pfb *Page) GetLSNBUffer() []byte {
	return pfb.buffer[4:8]
}

func (pfb *Page) CheckCRCMatch() bool {

	crc := crc32.ChecksumIEEE(pfb.GetPostCRCBuffer())
	crcMatch := crc == binary.BigEndian.Uint32(pfb.GetCheckSumBuffer())

	pfb.crcMatch = crcMatch

	return pfb.crcMatch
}

// TODO: MUST BE DONE
// add a new method to represent multiple page files creating a single block
// update the write method accrodingly
// we will require now to have LSN (last synced number) and also CSN (current sync number)
// this is to indicate the status of the page file block updated status
func (pfb *Page) SetPageBuffer(offset int, buffer []byte, currentLSN uint32) error {

	pfb.mutex.Lock()
	defer pfb.mutex.Unlock()

	if offset > len(pfb.buffer)-pageBufferBlockByteOffset || len(buffer) > len(pfb.buffer)-pageBufferBlockByteOffset {
		return ErrOutOfBounds
	}

	dataRegion := pfb.buffer[pageBufferBlockByteOffset+offset : pageBufferBlockByteOffset+offset+len(buffer)]
	copy(dataRegion, buffer)
	pfb.currentLSN = currentLSN
	pfb.dirty = true

	return nil
}

func (pfb *Page) serialize() []byte {
	pfb.mutex.RLock()
	defer pfb.mutex.RUnlock()

	if pfb.dirty {
		checksums.CalculateCRC(pfb.GetCheckSumBuffer(), pfb.GetPostCRCBuffer())
		binary.BigEndian.PutUint32(pfb.GetLSNBUffer(), pfb.currentLSN)
	}

	return pfb.buffer
}
