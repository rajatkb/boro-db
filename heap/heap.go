package heap

import (
	"boro-db/utils/checksums"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
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

type heapfilemeta struct {
	heapfileNumber int
	fd             int
	// serializable fields
	pageCount uint32
	buffer    []byte
}

func (hpm *heapfilemeta) Serialize() {
	binary.BigEndian.PutUint32(hpm.buffer[4:8], hpm.pageCount)
	checksums.CalculateCRC(hpm.buffer[0:4], hpm.buffer[4:])
}

func (hpm *heapfilemeta) Deserialize() error {
	hpm.pageCount = binary.BigEndian.Uint32(hpm.buffer[4:8])
	crcBuffer := make([]byte, 4)
	checksums.CalculateCRC(crcBuffer, hpm.buffer[4:])
	if !checksums.CompareCRC(crcBuffer, hpm.buffer[0:4]) {
		return fmt.Errorf("CRC mismatch")
	}
	return nil
}

func (hpm *heapfilemeta) SizeBytes(pageFileSize uint32) uint32 {
	return pageFileSize + hpm.pageCount*pageFileSize
}

func totalPagesInHeapFile(heapfileSize uint32, pageSize uint32) uint32 {
	return (heapfileSize - pageSize) / pageSize
}

type fileSystemHeap struct {
	logger                log.Logger
	option                FileOptions
	fileIdentifiers       []*heapfilemeta
	totalAddressablePages uint64

	totalPagesInHeapFile uint32

	heapFileLock *sync.RWMutex
}

// Truncate heap file to last page number
func (fsh *fileSystemHeap) Truncate(lastPageNumber uint64) error {

	// no better way for this yet
	// TODO :
	// ensure lock at heap file level not at a global level maybe
	fsh.heapFileLock.Lock()
	defer fsh.heapFileLock.Unlock()

	if fsh.totalAddressablePages < lastPageNumber {
		return nil
	}

	newLastPageNumber := lastPageNumber
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

		currentHeapFileStartPageNumber := uint64(fsh.totalPagesInHeapFile * uint32(i))

		if newLastPageNumber <= currentHeapFileStartPageNumber {
			// Delete everything in current file
			err := syscall.Unlink(filepath.Join(fsh.option.FileDirectory, heapFileName(i)))
			if err != nil {
				fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to delete heap file %d", i))
				return err
			}
			fsh.totalAddressablePages -= uint64(fsh.fileIdentifiers[i].pageCount)
			filesDeleted++
		} else {
			// Truncate the file
			newSize := newLastPageNumber - currentHeapFileStartPageNumber // making sure metadata is intact

			fsh.fileIdentifiers[i].pageCount = uint32(newSize)
			fsh.fileIdentifiers[i].Serialize()
			fsh.totalAddressablePages -= uint64(fsh.fileIdentifiers[i].pageCount)

			_, err := syscall.Pwrite(fsh.fileIdentifiers[i].fd, fsh.fileIdentifiers[i].buffer, 0)
			if err != nil {
				fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to write heap file %d", i))
				return err
			}

			err = syscall.Fsync(fsh.fileIdentifiers[i].fd)
			if err != nil {
				fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to fsync heap file %d", fsh.fileIdentifiers[i].heapfileNumber))
				return err
			}

			err = syscall.Ftruncate(fsh.fileIdentifiers[i].fd, int64(newSize*uint64(pageSize)+uint64(getHeapFileMetaSize(fsh.option))))
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

	fsh.heapFileLock.Lock()
	defer fsh.heapFileLock.Unlock()

	pagesRemainingToAllocate := uint64(pageCount)

	lastHeapFile := fsh.fileIdentifiers[len(fsh.fileIdentifiers)-1]
	for pagesRemainingToAllocate != 0 {

		if lastHeapFile.SizeBytes(fsh.option.PageSizeByte) == fsh.option.HeapFileSizeByte {
			// create a new heap file
			// extend size upto max heap file size of pageRemainingToAllocate whichever is lesser
			// update the new page count on disk
			// update the last heapfile
			// update the total addressable pages
			// reduce the pageRemainingToAllocate by that count

			extraPages := fsh.totalPagesInHeapFile

			extraPages = uint32(math.Min(float64(extraPages), float64(pagesRemainingToAllocate)))

			hpf, err := createNewEmptyHeapFile(len(fsh.fileIdentifiers), fsh.option, fsh.logger)

			if err != nil {
				fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to create new heap file %d", len(fsh.fileIdentifiers)))
				return err
			}

			err = fsh.allocatePagesInHeapFile(hpf, int64(hpf.SizeBytes(fsh.option.PageSizeByte)), int64(extraPages))

			if err != nil {
				fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to allocate pages in heap file %d", len(fsh.fileIdentifiers)))
				return err
			}

			fsh.fileIdentifiers = append(fsh.fileIdentifiers, hpf)
			lastHeapFile = hpf
			pagesRemainingToAllocate -= uint64(extraPages)
			fsh.totalAddressablePages += uint64(extraPages)
		} else {
			// extend current heap file to max heap file size in the file
			// reduce page Remaining to allocate by that count
			// increase the pageCount of this last page and the total adddressable pages

			heapFileSize := int64(lastHeapFile.SizeBytes(fsh.option.PageSizeByte))

			extraPages := (int64(fsh.option.HeapFileSizeByte) - heapFileSize) / int64(fsh.option.PageSizeByte)

			extraPages = int64(math.Min(float64(extraPages), float64(pagesRemainingToAllocate)))

			err := fsh.allocatePagesInHeapFile(lastHeapFile, heapFileSize, extraPages)
			if err != nil {
				return err
			}

			fsh.totalAddressablePages += uint64(extraPages)
			pagesRemainingToAllocate -= uint64(extraPages)
		}

	}

	return nil
}

func (fsh *fileSystemHeap) allocatePagesInHeapFile(hpf *heapfilemeta, heapFileSize int64, extraPages int64) error {
	err := syscall.Fallocate(hpf.fd, 0, heapFileSize, extraPages*int64(fsh.option.PageSizeByte))
	if err != nil {
		fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to extend heap file %d", hpf.heapfileNumber))
		return err
	}
	hpf.pageCount += uint32(extraPages)
	hpf.Serialize()
	_, err = syscall.Pwrite(hpf.fd, hpf.buffer, 0)
	if err != nil {
		hpf.pageCount -= uint32(extraPages)
		fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to write heap file %d", hpf.heapfileNumber))
		return err
	}

	err = syscall.Fsync(hpf.fd)
	if err != nil {
		fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to fsync heap file %d", hpf.heapfileNumber))
		return err
	}

	return nil
}

func (fsh *fileSystemHeap) Read(pageNumber uint64, buffer []byte, onRead func(error)) {

	heapFile := pageNumber / uint64(fsh.totalPagesInHeapFile)
	heapFileOffset := pageNumber % uint64(fsh.totalPagesInHeapFile)

	_, err := syscall.Pread(fsh.fileIdentifiers[heapFile].fd, buffer, int64(getHeapFileMetaSize(fsh.option))+int64(heapFileOffset*uint64(fsh.option.PageSizeByte)))

	onRead(err)
}

func (fsh *fileSystemHeap) Write(pageNumber uint64, buffer []byte, onWrite func(error)) {

	heapFile := pageNumber / uint64(fsh.option.HeapFileSizeByte/fsh.option.PageSizeByte)
	heapFileOffset := pageNumber % uint64(fsh.option.HeapFileSizeByte/fsh.option.PageSizeByte)

	_, err := syscall.Pwrite(fsh.fileIdentifiers[heapFile].fd, buffer, int64(getHeapFileMetaSize(fsh.option))+int64(heapFileOffset*uint64(fsh.option.PageSizeByte)))

	if err := syscall.Fsync(fsh.fileIdentifiers[heapFile].fd); err != nil {
		fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to fsync heap file %d", heapFile))
		onWrite(err)
		return
	}

	onWrite(err)
}

func (fsh *fileSystemHeap) MaxAddressablePage() uint64 {
	fsh.heapFileLock.Lock()
	defer fsh.heapFileLock.Unlock()
	return fsh.totalAddressablePages
}

func getHeapFileMetaSize(option FileOptions) uint32 {
	return uint32(option.PageSizeByte)
}

/*
Creates heapfile in sequence , starts with a heap file of size page size
if list of heap file is empty. If not loads the heapfile file pointer
and stores them.
*/
func NewHeap(logger log.Logger, option FileOptions) (HeapFile, error) {

	// create heap meta file to lock the heap file status like
	// pageFileSize
	// heapFileMaxSize
	// - these values can never change
	// - any artifact trying to read this in any other manner will tamper the heap file

	heapFileMetaSize := getHeapFileMetaSize(option)

	_, err := os.Stat(option.FileDirectory)

	if err != nil {
		logger.Error().Err(err).Msg("Creating heap file directory")
		err = os.Mkdir(option.FileDirectory, os.ModePerm)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to create heap file directory")
			return nil, err
		}
	}

	fileEntries, err := os.ReadDir(option.FileDirectory)

	if err != nil {
		logger.Error().Err(err).Msg("Failed to read heap file list")
		return nil, err
	}

	fileIdentifiers := make([]*heapfilemeta, 0, len(fileEntries))
	fileIdentifiersMap := make(map[string]*heapfilemeta)
	totalAddressablePages := uint64(0)
	for _, fileEntry := range fileEntries {
		if fileEntry.IsDir() {
			continue
		}
		if strings.Contains(fileEntry.Name(), "heap") {

			// get all of the heap files
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

			// stash the fd and heap file number
			hpf := &heapfilemeta{
				fd:             fd,
				heapfileNumber: int(fileNumber),
			}

			fileIdentifiersMap[fileEntry.Name()] = hpf
		}
	}

	for _, hpf := range fileIdentifiersMap {

		buffer := make([]byte, option.PageSizeByte)
		_, err := syscall.Pread(hpf.fd, buffer, 0)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to read heap file")
			return nil, err
		}
		hpf.buffer = buffer
		stat, err := os.Stat(filepath.Join(option.FileDirectory, heapFileName(hpf.heapfileNumber)))
		// corrects the file meta based on the file size
		// further correction logic can involve reading all the pages and checking how many of them are legit
		// pages and then truncating them off
		// TODO:
		// potential issue of partial page writes is not handled. So if the heap has bunch of corrupted pages
		// its upto page manager to handle it. or the system can be set in RAID 1
		if hpf.Deserialize() != nil {

			if err != nil {
				logger.Error().Err(err).Msg("Failed to get stat of heap file")
				return nil, err
			}

			// correction phase
			// confirm if size is a multiple of page size + heapMetaSize
			if (stat.Size()-int64(heapFileMetaSize))%int64(option.PageSizeByte) == 0 {
				// correct the page file
				totalPages := (stat.Size() - int64(heapFileMetaSize)) / int64(option.PageSizeByte)
				hpf.pageCount = uint32(totalPages)
			}

			hpf.Serialize()
			_, err = syscall.Pwrite(hpf.fd, hpf.buffer, 0)
			if err != nil {
				logger.Error().Err(err).Msg("Failed to write heap file")
				return nil, err
			}
			if err := syscall.Fsync(hpf.fd); err != nil {
				logger.Error().Err(err).Msg(fmt.Sprintf("Failed to fsync heap file %d", hpf.heapfileNumber))
				return nil, err
			}
			hpf.Deserialize()
		}

		// pageCount can never be larger than the HeapFiles current pages
		// Since in Truncate command we first commit the page size information
		// and only then truncate the file. This ensures when recoveruing we end up with
		// a consistent heap file.
		err = syscall.Ftruncate(hpf.fd, int64(hpf.pageCount)*int64(option.PageSizeByte)+int64(heapFileMetaSize))
		if err != nil {
			logger.Error().Err(err).Msg("Failed to truncate heap file")
			return nil, err
		}

		totalAddressablePages += uint64(hpf.pageCount)
		fileIdentifiers = append(fileIdentifiers, hpf)
	}

	sort.Slice(fileIdentifiers, func(i, j int) bool {
		return fileIdentifiers[i].heapfileNumber < fileIdentifiers[j].heapfileNumber
	})

	if len(fileIdentifiers) == 0 {
		hpm, err := createNewEmptyHeapFile(0, option, logger)
		if err != nil {
			return nil, err
		}

		// TODO : correct file size based on meta
		fileIdentifiers = append(fileIdentifiers, hpm)
	}

	return &fileSystemHeap{
		logger:                logger,
		fileIdentifiers:       fileIdentifiers,
		totalAddressablePages: totalAddressablePages,
		totalPagesInHeapFile:  totalPagesInHeapFile(option.HeapFileSizeByte, option.PageSizeByte),
		heapFileLock:          &sync.RWMutex{},
		option:                option,
	}, nil
}

func createNewEmptyHeapFile(heapfileIndex int, option FileOptions, logger log.Logger) (*heapfilemeta, error) {

	heapFileMetaSize := getHeapFileMetaSize(option)

	fd, err := syscall.Open(filepath.Join(option.FileDirectory, heapFileName(heapfileIndex)), syscall.O_RDWR|syscall.O_DSYNC|syscall.O_CREAT, permissionBits)
	if err != nil {
		logger.Error().Err(err).Msg(fmt.Sprintf("Failed to open heap file %d", heapfileIndex))
		return nil, err
	}

	err = syscall.Fallocate(fd, 0, 0, int64(heapFileMetaSize))
	if err != nil {
		logger.Error().Err(err).Msg(fmt.Sprintf("Failed to allocate page in heap file %d", heapfileIndex))
		return nil, err
	}

	hpm := &heapfilemeta{
		pageCount:      0,
		fd:             fd,
		heapfileNumber: heapfileIndex,
	}

	hpm.buffer = make([]byte, heapFileMetaSize)
	hpm.Serialize()

	_, err = syscall.Pwrite(fd, hpm.buffer, 0)

	if err != nil {
		logger.Error().Err(err).Msg(fmt.Sprintf("Failed to write heap file %d", heapfileIndex))
		return nil, err
	}
	if err := syscall.Fsync(fd); err != nil {
		logger.Error().Err(err).Msg(fmt.Sprintf("Failed to fsync heap file %d", heapfileIndex))
		return nil, err
	}

	return hpm, nil
}

func heapFileName(number int) string {
	return fmt.Sprintf("%s%s%d", heapFileNamePrefix, heapfileNameSepparate, number)
}
