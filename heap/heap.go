package heap

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/phuslu/log"
)

/*
Heap file
┌──────────────────────────────────────────────────────────────┐
| crc (4byte) | pageCount (4byte) |                            |
|──────────────────────4kb metadata────────────────────────────|
| ......                                                       |
└──────────────────────────────────────────────────────────────┘
*/

const permissionBits = 0755 // directory requires executioin as well hence 7 bit
const heapFileNamePrefix = "heapFile"
const heapfileNameSepparate = "-"

const heapFileMetaSize = 4096

type heapfilemeta struct {
	heapfileNumber int
	pageCount      uint32
	fd             int
}

func (hpm *heapfilemeta) Serialize(buffer []byte) {
	binary.BigEndian.PutUint32(buffer[4:8], hpm.pageCount)
	calculateCRC(buffer[0:4], buffer[4:])
}

func (hpm *heapfilemeta) Deserialize(buffer []byte) error {
	hpm.pageCount = binary.BigEndian.Uint32(buffer[0:4])
	crcBuffer := make([]byte, 4)
	calculateCRC(crcBuffer, buffer[4:])
	if !compareCRC(crcBuffer, buffer[0:4]) {
		return fmt.Errorf("CRC mismatch")
	}
	return nil
}

func totalPagesInHeapFile(heapfileSize uint32, pageSize uint32) uint32 {
	return (heapfileSize - heapFileMetaSize) / pageSize
}

type fileSystemHeap struct {
	logger                log.Logger
	option                FileOptions
	fileIdentifiers       []heapfilemeta
	totalAddressablePages uint64

	totalPagesInHeapFile uint32
}

// Truncate heap file to last page number
func (fsh *fileSystemHeap) Truncate(lastPageNumber uint64) error {

	if fsh.totalAddressablePages < lastPageNumber {
		return nil
	}

	newLastPageNumber := fsh.totalAddressablePages - lastPageNumber
	filesDeleted := 0

	/*
		deletes / truncates file as per page Count provided
		where pageRangeTodelete = MaxPageCount - 100 + MaxPageCount + 100
		heapFile 1 - 100 - MaxPageCount
		heapfile 2 - 0 - MaxPageCount
		heapfile 3 - 0 - 100
	*/

	for i := len(fsh.fileIdentifiers) - 1; i >= 0; i-- {

		pageSize := fsh.option.PageSizeByte

		currentHeapFileStartPageNumber := uint64(totalPagesInHeapFile(fsh.option.HeapFileSizeByte, fsh.option.PageSizeByte) * uint32(i))

		if newLastPageNumber <= currentHeapFileStartPageNumber {
			// Delete everything in current file
			err := syscall.Unlink(filepath.Join(fsh.option.FileDirectory, heapFileName(i)))
			if err != nil {
				fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to delete heap file %d", i))
				return err
			}
			filesDeleted++
		} else {
			// Truncate the file
			newSize := newLastPageNumber - currentHeapFileStartPageNumber
			err := syscall.Ftruncate(fsh.fileIdentifiers[i].fd, int64(newSize*uint64(pageSize)))
			if err != nil {
				fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to truncate heap file %d", i))
				return err
			}
			break
		}

	}

	fsh.fileIdentifiers = fsh.fileIdentifiers[:len(fsh.fileIdentifiers)-filesDeleted]
	fsh.totalAddressablePages = lastPageNumber

	return nil
}

// Add new heap file or extend existing one based on page count
func (fsh *fileSystemHeap) AllocatePage(pageCount int) error {

	lastHeapfilePtr := len(fsh.fileIdentifiers) - 1

	stat, err := os.Stat(filepath.Join(fsh.option.FileDirectory, heapFileName(lastHeapfilePtr)))
	if err != nil {
		fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to get heap file stat:  %d", lastHeapfilePtr))
		return err
	}

	totalNewHeapFiles := uint32(pageCount) / fsh.totalPagesInHeapFile
	// remainingSize := stat.Size() % int64(fsh.option.HeapFileSizeByte)

	if totalNewHeapFiles > 0 {

		for i := 0; i < int(totalNewHeapFiles); i++ {
			fd, err := syscall.Open(filepath.Join(fsh.option.FileDirectory, heapFileName(lastHeapfilePtr+1)), syscall.O_RDWR|syscall.O_DSYNC|syscall.O_CREAT, permissionBits)
			if err != nil {
				fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to open heap file %d", i))
				return err
			}
			err = syscall.Fallocate(fd, 0, 0, int64(fsh.option.HeapFileSizeByte))
			if err != nil {
				fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to allocate page in heap file %d", i))
				return err
			}

			buffer := make([]byte, fsh.option.HeapFileSizeByte)
			putEmptyHeapPage(buffer[heapFileMetaSize:], fsh.option.PageSizeByte, int(fsh.totalPagesInHeapFile))
			_, err = syscall.Pwrite(fd, buffer, heapFileMetaSize)
			if err != nil {
				fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to write to heap file %d", i))
				return err
			}

			// ensuring heapfile meta is consistent

			hpm := heapfilemeta{
				pageCount: fsh.option.HeapFileSizeByte,
				fd:        fd,
			}
			hpm.Serialize(buffer[0:heapFileMetaSize])
			_, err = syscall.Pwrite(fd, buffer[0:heapFileMetaSize], heapFileMetaSize)
			if err != nil {
				fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to write to heap file %d", i))
				return err
			}

			fsh.fileIdentifiers = append(fsh.fileIdentifiers, hpm)
		}

	}

	// if sizeToExtend := math.Min(float64(fsh.option.HeapFileSizeByte-uint32(stat.Size())), float64(remainingSize)); sizeToExtend > 0 {

	// 	err := syscall.Fallocate(fsh.fileIdentifiers[lastHeapfilePtr].fd, 0, stat.Size(), int64(sizeToExtend))
	// 	if err != nil {
	// 		fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to allocate page in heap file %d", lastHeapfilePtr))
	// 		return err
	// 	}

	// 	buffer := make([]byte, int(sizeToExtend))
	// 	putEmptyHeapPage(buffer, uint64(fsh.option.PageSizeByte), int(sizeToExtend)/int(fsh.option.PageSizeByte))
	// 	_, err = syscall.Pwrite(fsh.fileIdentifiers[lastHeapfilePtr].fd, buffer, int64(stat.Size()))
	// 	if err != nil {
	// 		fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to write to heap file %d", lastHeapfilePtr))
	// 		return err
	// 	}

	// 	if extraRemaining := remainingSize - int64(sizeToExtend); extraRemaining > 0 {
	// 		fd, err := syscall.Open(filepath.Join(fsh.option.FileDirectory, heapFileName(lastHeapfilePtr+int(totalNewHeapFiles)+1)), syscall.O_RDWR|syscall.O_DSYNC|syscall.O_CREAT, permissionBits)
	// 		if err != nil {
	// 			fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to open heap file %d", lastHeapfilePtr+int(totalNewHeapFiles)+1))
	// 			return err
	// 		}
	// 		err = syscall.Fallocate(fd, 0, 0, extraRemaining)
	// 		if err != nil {
	// 			fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to allocate page in heap file %d", lastHeapfilePtr+int(totalNewHeapFiles)+1))
	// 			return err
	// 		}

	// 		buffer := make([]byte, fsh.option.PageSizeByte*uint32(pageCount))
	// 		putEmptyHeapPage(buffer, uint64(fsh.option.PageSizeByte), pageCount)
	// 		syscall.Pwrite(fsh.fileIdentifiers[lastHeapfilePtr].fd, buffer,0)

	// 		fsh.fileIdentifiers = append(fsh.fileIdentifiers, heapfilemeta{
	// 			pageCount:
	// 		})
	// 	}
	// }

	fsh.totalAddressablePages += uint64(pageCount)

	return err
}

func (fsh *fileSystemHeap) Read(pageNumber uint64, onRead func(*PageFileBlock, error)) {

	heapFile := pageNumber / uint64(fsh.totalPagesInHeapFile)
	heapFileOffset := pageNumber % uint64(fsh.totalPagesInHeapFile)

	buffer := make([]byte, fsh.option.PageSizeByte)

	_, err := syscall.Pread(fsh.fileIdentifiers[heapFile].fd, buffer, int64(heapFileOffset*uint64(fsh.option.PageSizeByte)))

	onRead(readPageFileBlock(buffer, uint64(fsh.option.PageSizeByte)), err)
}

func (fsh *fileSystemHeap) Write(pageNumber uint64, pfb *PageFileBlock, onWrite func(error)) {

	heapFile := pageNumber / uint64(fsh.option.HeapFileSizeByte/fsh.option.PageSizeByte)
	heapFileOffset := pageNumber % uint64(fsh.option.HeapFileSizeByte/fsh.option.PageSizeByte)

	buffer := pfb.Serialize()

	_, err := syscall.Pwrite(fsh.fileIdentifiers[heapFile].fd, buffer, int64(heapFileOffset*uint64(fsh.option.PageSizeByte)))

	onWrite(err)
}

func (fsh *fileSystemHeap) MaxAddressablePage() uint64 {

	return fsh.totalAddressablePages
}

/*
Creates heapfile in sequence , starts with a heap file of size page size
if list of heap file is empty. If not loads the heapfile file pointer
and stores them.
*/
func NewHeap(logger log.Logger, option FileOptions) (HeapFile, error) {

	fileEntries, err := os.ReadDir(option.FileDirectory)

	if err != nil {
		logger.Error().Err(err).Msg("Failed to read heap file list")
		return nil, err
	}

	fileIdentifiers := make([]heapfilemeta, 0, len(fileEntries))
	fileIdentifiersMap := make(map[string]heapfilemeta)
	totalAddressablePages := uint64(0)
	for _, fileEntry := range fileEntries {
		if fileEntry.IsDir() {
			continue
		}
		if strings.Contains(fileEntry.Name(), "heap") {
			fileLocation := filepath.Join(option.FileDirectory, fileEntry.Name())
			logger.Info().Str("file", fileLocation).Msg(fmt.Sprintf("Found heap file %s", fileEntry.Name()))

			fd, err := syscall.Open(fileLocation, syscall.O_RDWR|syscall.O_DSYNC, permissionBits)

			if err != nil {
				logger.Error().Err(err).Msg(fmt.Sprintf("Failed to open heap file %s", fileEntry.Name()))
				return nil, err
			}

			fileNumber, err := strconv.ParseInt(strings.Split(fileEntry.Name(), heapfileNameSepparate)[1], 10, 64)

			if err != nil {
				logger.Error().Err(err).Msg(fmt.Sprintf("Failed to parse heap file number %s", fileEntry.Name()))
				return nil, err
			}

			hpf := heapfilemeta{
				fd:             fd,
				heapfileNumber: int(fileNumber),
			}
			fileIdentifiersMap[fileEntry.Name()] = hpf
		}
	}

	// TODO :
	// load each heapfile
	// check meta CRC , if correct load page count and truncate files to ensure correct sizes
	// if incorrect check size of file if it only has metadata delete the file
	// if size is large and has other data as well do a checksum check for every page and update size data accordingly

	for _, hpf := range fileIdentifiersMap {

		buffer := make([]byte, heapFileMetaSize)
		_, err := syscall.Pread(hpf.fd, buffer, 0)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to read heap file")
			return nil, err
		}
		if hpf.Deserialize(buffer) != nil {
			// heap file is potentially corrupted -> must fix or deleted
		}

		err = syscall.Ftruncate(hpf.fd, int64(hpf.pageCount*option.PageSizeByte))
		if err != nil {
			logger.Error().Err(err).Msg("Failed to truncate heap file")
			return nil, err
		}
		fileIdentifiers = append(fileIdentifiers, hpf)
	}

	sort.Slice(fileIdentifiers, func(i, j int) bool {
		return fileIdentifiers[i].heapfileNumber < fileIdentifiers[j].heapfileNumber
	})

	if len(fileIdentifiers) == 0 {
		fd, err := syscall.Open(filepath.Join(option.FileDirectory, heapFileName(0)), syscall.O_RDWR|syscall.O_DSYNC|syscall.O_CREAT, permissionBits)
		if err != nil {
			logger.Error().Err(err).Msg(fmt.Sprintf("Failed to open heap file %d", 0))
			return nil, err
		}

		err = syscall.Fallocate(fd, 0, 0, int64(heapFileMetaSize))
		if err != nil {
			logger.Error().Err(err).Msg(fmt.Sprintf("Failed to allocate page in heap file %d", i))
			return nil, err
		}

		hpm := heapfilemeta{
			pageCount: 0,
			fd:        fd,
		}

		buffer := make([]byte, heapFileMetaSize)

		hpm.Serialize(buffer)

		_, err = syscall.Pwrite(fd, buffer, 0)

		// TODO : correct file size based on meta
		fileIdentifiers = append(fileIdentifiers, hpm)
	}

	return &fileSystemHeap{
		logger:                logger,
		fileIdentifiers:       fileIdentifiers,
		totalAddressablePages: totalAddressablePages,
		totalPagesInHeapFile:  totalPagesInHeapFile(option.HeapFileSizeByte, option.PageSizeByte),
	}, nil
}
