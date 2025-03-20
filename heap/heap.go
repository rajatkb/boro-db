package heap

import (
	"boro-db/utils/checksums"
	"boro-db/utils/freelist"
	"encoding/binary"
	"errors"
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

var ErrNotEnoughSpace = fmt.Errorf("not enough space")

/*
Heap file
┌──────────────────────────────────────────────────────────────┐
| crc (4byte) | pageCount (4byte) |                            |
| start-address (8byte)                                        |
|──────────────────────4kb metadata────────────────────────────|
| (((HeapSize - PageSize) / PagSize) / 8) / PageSize = freePage|
| used for tracking free pages                                 |
|--------------------------------------------------------------|
| ......                                                       |
└──────────────────────────────────────────────────────────────┘
*/

const permissionBits = 0755 // directory requires executioin as well hence 7 bit
const heapFileNamePrefix = "heapFile"
const heapfileNameSepparate = "-"

type heapfilemeta struct {
	addressSpaceStart uint64
	fd                int
	// serializable fields
	pageCount uint32
	buffer    []byte
	freelist  []freelist.FreeList
	options   *HeapFileOptions
}

func (hpm *heapfilemeta) SerializeMetaData() {
	binary.BigEndian.PutUint32(hpm.buffer[4:8], hpm.pageCount)
	binary.BigEndian.PutUint64(hpm.buffer[8:16], hpm.addressSpaceStart)
	checksums.CalculateCRC(hpm.buffer[0:4], hpm.buffer[4:hpm.options.PageSizeByte])
}

func (hpm *heapfilemeta) DeserializeMetadat() error {
	hpm.pageCount = binary.BigEndian.Uint32(hpm.buffer[4:8])
	hpm.addressSpaceStart = binary.BigEndian.Uint64(hpm.buffer[8:16])
	crcBuffer := make([]byte, 4)
	checksums.CalculateCRC(crcBuffer, hpm.buffer[4:hpm.options.PageSizeByte])

	if !checksums.CompareCRC(crcBuffer, hpm.buffer[0:4]) {
		return fmt.Errorf("CRC mismatch")
	}
	return nil
}

func (hpm *heapfilemeta) SizeBytes() uint32 {
	return getHeapFileMetaSize(hpm.options) + hpm.pageCount*hpm.options.PageSizeByte
}

func totalPagesInHeapFile(option *HeapFileOptions) uint32 {
	return (option.HeapFileSizeByte - getHeapFileMetaSize(option)) / option.PageSizeByte
}

func getFreeListSizeBytes(option *HeapFileOptions) uint32 {
	maxBytesNeeded := math.Ceil(float64(option.HeapFileSizeByte-option.PageSizeByte) / (8 * float64(option.PageSizeByte)))
	return uint32(math.Ceil(maxBytesNeeded/float64(option.PageSizeByte))) * option.PageSizeByte
}

func getHeapFileMetaSize(option *HeapFileOptions) uint32 {
	// add the free page size requirement per heap file
	freeListSize := getFreeListSizeBytes(option)

	return uint32(option.PageSizeByte) + freeListSize
}

type fileSystemHeap struct {
	logger                     log.Logger
	option                     *HeapFileOptions
	fileIdentifiers            []*heapfilemeta
	firstAddressInAddressSpace uint64
	lastAddressInAddressSpace  uint64
	startAddressMap            map[uint64]*heapfilemeta
	maxTotalPagesInHeapFile    uint32
	heapMetaSize               uint32
	heapFileLock               *sync.RWMutex
}

func (fsh *fileSystemHeap) IsPageFree(pageNumber uint64) bool {
	fsh.heapFileLock.RLock()
	defer fsh.heapFileLock.RUnlock()

	if pageNumber < fsh.firstAddressInAddressSpace || pageNumber > fsh.lastAddressInAddressSpace {
		return false
	}

	heapFileStartAddress := pageNumber - pageNumber%uint64(fsh.maxTotalPagesInHeapFile)

	heapFile := fsh.startAddressMap[heapFileStartAddress]

	pageOffset := pageNumber % uint64(fsh.maxTotalPagesInHeapFile)

	freeListIdx := pageOffset / uint64(fsh.option.PageSizeByte*8)
	freeListSlot := pageOffset % uint64(fsh.option.PageSizeByte*8)

	return heapFile.freelist[freeListIdx].IsPageFree(freeListSlot)
}

func (fsh *fileSystemHeap) FreePagesAvailable() uint64 {
	size := 0
	for _, hpf := range fsh.fileIdentifiers {
		for _, fl := range hpf.freelist {
			size += int(fl.FreePagesAvailable())
		}
	}
	return uint64(size)
}

func (fsh *fileSystemHeap) Free(pageNumbers []uint64) error {
	fsh.heapFileLock.Lock()
	defer fsh.heapFileLock.Unlock()
	fsh.logger.Debug().Msgf("Free %d pages heap : %s", len(pageNumbers), fsh.option.FileDirectory)
	freeListSizeBytes := getFreeListSizeBytes(fsh.option)
	freeListToSync := make(map[*heapfilemeta][][]uint64)

	for _, pageNumber := range pageNumbers {
		heapFileStartAddress := pageNumber - pageNumber%uint64(fsh.maxTotalPagesInHeapFile)
		heapFileMeta, ok := fsh.startAddressMap[heapFileStartAddress]
		if !ok {
			fsh.logger.Debug().Msgf("Page number %d not found in heap file : %s", pageNumber, fsh.option.FileDirectory)
			continue
		}
		pageOffset := pageNumber % uint64(fsh.maxTotalPagesInHeapFile)

		if pageOffset >= uint64(heapFileMeta.pageCount) {
			fsh.logger.Debug().Msgf("Page number %d not found in heap file : %s", pageNumber, fsh.option.FileDirectory)
			continue
		}

		freeListIdx := pageOffset / uint64(fsh.option.PageSizeByte*8)
		freeListSlot := pageOffset % uint64(fsh.option.PageSizeByte*8)

		if _, ok := freeListToSync[heapFileMeta]; !ok {
			freeListToSync[heapFileMeta] = make([][]uint64, freeListSizeBytes/fsh.option.PageSizeByte)
		}
		freeListToSync[heapFileMeta][freeListIdx] = append(freeListToSync[heapFileMeta][freeListIdx], freeListSlot)
	}

	for heapFileMeta, freeListIdxs := range freeListToSync {
		for idx, pages := range freeListIdxs {
			if len(pages) != 0 {

				heapFileMeta.freelist[idx].ReleasePages(pages)

				freePageDataSpace := heapFileMeta.buffer[fsh.option.PageSizeByte:freeListSizeBytes]

				currentFreePageBuffer := freePageDataSpace[idx*int(fsh.option.PageSizeByte) : (idx+1)*int(fsh.option.PageSizeByte)]

				_, err := syscall.Pwrite(heapFileMeta.fd, currentFreePageBuffer, int64((idx+1)*int(fsh.option.PageSizeByte)))

				if err != nil {
					heapFileMeta.freelist[idx].GetPages(uint64(len(pages))) // grab the same pages in exact same order
					fsh.logger.Error().Msgf("Error writing to heap file : %s", err.Error())
					return err
				}
			}
		}
	}

	return nil
}

func (fsh *fileSystemHeap) Malloc(pageCount uint64) ([]uint64, error) {
	fsh.heapFileLock.Lock()
	defer fsh.heapFileLock.Unlock()

	remainingPages := pageCount

	for _, hpf := range fsh.fileIdentifiers {
		for _, freeList := range hpf.freelist {
			remainingPages -= freeList.FreePagesAvailable()
			if remainingPages <= 0 {
				break
			}
		}
	}

	if remainingPages > 0 {
		return nil, ErrNotEnoughSpace
	}
	freeListSizeBytes := getFreeListSizeBytes(fsh.option)
	remainingPages = pageCount
	finalPages := make([]uint64, pageCount)

	j := 0

	for _, hpf := range fsh.fileIdentifiers {
		for idx, freeList := range hpf.freelist {
			pages, _ := freeList.GetPages(remainingPages)

			freePageDataSpace := hpf.buffer[fsh.option.PageSizeByte:freeListSizeBytes]
			currentFreePageBuffer := freePageDataSpace[idx*int(fsh.option.PageSizeByte) : (idx+1)*int(fsh.option.PageSizeByte)]

			_, err := syscall.Pwrite(hpf.fd, currentFreePageBuffer, int64((idx+1)*int(fsh.option.PageSizeByte)))

			if err != nil {
				fsh.logger.Error().Err(err).Msg("Error while writing FreeSpaceInfo to heap file")
				freeList.ReleasePages(pages)
				return nil, err
			}

			for i := 0; i < len(pages); i++ {
				pages[i] = hpf.addressSpaceStart + uint64(idx)*uint64(fsh.option.PageSizeByte*8) + pages[i]
				finalPages[j] = pages[i]
				j++
			}
			remainingPages -= uint64(len(pages))
			if remainingPages <= 0 {
				break
			}
		}
	}
	fsh.logger.Debug().Msgf("Malloc %d pages heap : %s", pageCount, fsh.option.FileDirectory)

	return finalPages, nil
}

func (fsh *fileSystemHeap) TrimHead(count uint64) error {

	// no better way for this yet
	// TODO :
	// ensure lock at heap file level not at a global level maybe
	fsh.heapFileLock.Lock()
	defer fsh.heapFileLock.Unlock()

	if fsh.lastAddressInAddressSpace-fsh.firstAddressInAddressSpace+1 < count {
		return fmt.Errorf("cannot trim heap file to less than %d pages", count)
	}

	newLastPageNumber := fsh.lastAddressInAddressSpace - count

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

		currentHeapFileStartPageNumber := fsh.fileIdentifiers[i].addressSpaceStart

		if newLastPageNumber < currentHeapFileStartPageNumber {
			// Delete everything in current file
			err := syscall.Unlink(filepath.Join(fsh.option.FileDirectory, heapFileName(currentHeapFileStartPageNumber)))
			if err != nil {
				fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to delete heap file %d", i))
				return err
			}
			fsh.lastAddressInAddressSpace -= uint64(fsh.fileIdentifiers[i].pageCount)
			delete(fsh.startAddressMap, currentHeapFileStartPageNumber)
			filesDeleted++
		} else {
			// Truncate the file
			newSize := newLastPageNumber - currentHeapFileStartPageNumber + 1 // making sure metadata is intact
			prevPageCount := fsh.fileIdentifiers[i].pageCount
			fsh.fileIdentifiers[i].pageCount = uint32(newSize)
			fsh.fileIdentifiers[i].SerializeMetaData()
			_, err := syscall.Pwrite(fsh.fileIdentifiers[i].fd, fsh.fileIdentifiers[i].buffer, 0)
			if err != nil {
				fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to write heap file %d", i))
				fsh.fileIdentifiers[i].pageCount = prevPageCount // restore values
				return err
			}
			err = syscall.Fsync(fsh.fileIdentifiers[i].fd)
			if err != nil {
				fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to fsync heap file %d", fsh.fileIdentifiers[i].addressSpaceStart))
				fsh.fileIdentifiers[i].pageCount = prevPageCount // restore values
				return err
			}

			fsh.lastAddressInAddressSpace -= uint64(prevPageCount - uint32(newSize))

			err = syscall.Ftruncate(fsh.fileIdentifiers[i].fd, int64(newSize*uint64(pageSize)+uint64(fsh.heapMetaSize)))
			if err != nil {
				fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to truncate heap file %d", i))
				return err
			}

			createFreeSizePages(fsh.fileIdentifiers[i], getHeapFileMetaSize(fsh.option), fsh.option)

			break
		}
	}

	fsh.fileIdentifiers = fsh.fileIdentifiers[:len(fsh.fileIdentifiers)-filesDeleted]

	return nil
}

func (fsh *fileSystemHeap) TrimTailHeapFiles(count uint64) error {

	// no better way for this yet
	// TODO :
	// ensure lock at heap file level not at a global level maybe
	fsh.heapFileLock.Lock()
	defer fsh.heapFileLock.Unlock()

	if fsh.lastAddressInAddressSpace-fsh.firstAddressInAddressSpace+1 < count {
		return fmt.Errorf("cannot trim heap file to less than %d pages", count)
	}

	newFirstPageNumber := fsh.firstAddressInAddressSpace + count

	filesDeleted := 0

	/*
		deletes / truncates file as per page Count provided
		where pageRangeTodelete = MaxPageCount - 100 + MaxPageCount + 100
		heapFile 1 - 100 - MaxPageCount
		heapfile 2 - 0 - MaxPageCount
		heapfile 3 - 0 - 100
	*/

	for i := 0; i < len(fsh.fileIdentifiers); i-- {

		currentHeapFileLastPageNumber := fsh.fileIdentifiers[i].addressSpaceStart + uint64(fsh.fileIdentifiers[i].pageCount) - 1

		if newFirstPageNumber > currentHeapFileLastPageNumber {
			// Delete everything in current file
			err := syscall.Unlink(filepath.Join(fsh.option.FileDirectory, heapFileName(currentHeapFileLastPageNumber)))
			if err != nil {
				fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to delete heap file %d", i))
				return err
			}
			fsh.firstAddressInAddressSpace += uint64(fsh.fileIdentifiers[i].pageCount)
			delete(fsh.startAddressMap, currentHeapFileLastPageNumber)
			filesDeleted++
		}
	}

	fsh.fileIdentifiers = fsh.fileIdentifiers[filesDeleted:]

	return nil
}

// Add new heap file or extend existing one based on page count
func (fsh *fileSystemHeap) ExtendBy(pageCount int) error {

	fsh.heapFileLock.Lock()
	defer fsh.heapFileLock.Unlock()

	pagesRemainingToAllocate := uint64(pageCount)

	lastHeapFile := fsh.fileIdentifiers[len(fsh.fileIdentifiers)-1]
	for pagesRemainingToAllocate != 0 {

		if lastHeapFile.SizeBytes() == fsh.option.HeapFileSizeByte {
			// create a new heap file
			// extend size upto max heap file size of pageRemainingToAllocate whichever is lesser
			// update the new page count on disk
			// update the last heapfile
			// update the total addressable pages
			// reduce the pageRemainingToAllocate by that count

			extraPages := fsh.maxTotalPagesInHeapFile

			extraPages = uint32(math.Min(float64(extraPages), float64(pagesRemainingToAllocate)))

			hpf, err := createNewEmptyHeapFile(lastHeapFile.addressSpaceStart+uint64(lastHeapFile.pageCount), fsh.option, fsh.logger)

			if err != nil {
				fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to create new heap file %d", len(fsh.fileIdentifiers)))
				return err
			}

			err = fsh.allocatePagesInHeapFile(hpf, int64(hpf.SizeBytes()), int64(extraPages))

			if err != nil {
				fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to allocate pages in heap file %d", len(fsh.fileIdentifiers)))
				return err
			}

			fsh.fileIdentifiers = append(fsh.fileIdentifiers, hpf)
			lastHeapFile = hpf
			pagesRemainingToAllocate -= uint64(extraPages)
			fsh.lastAddressInAddressSpace += uint64(extraPages)

			fsh.startAddressMap[hpf.addressSpaceStart] = hpf

		} else {
			// extend current heap file to max heap file size in the file
			// reduce page Remaining to allocate by that count
			// increase the pageCount of this last page and the total adddressable pages

			heapFileSize := int64(lastHeapFile.SizeBytes())

			extraPages := (int64(fsh.option.HeapFileSizeByte) - heapFileSize) / int64(fsh.option.PageSizeByte)

			extraPages = int64(math.Min(float64(extraPages), float64(pagesRemainingToAllocate)))

			err := fsh.allocatePagesInHeapFile(lastHeapFile, heapFileSize, extraPages)
			if err != nil {
				return err
			}

			// since the size has increased we will recreate the free space
			createFreeSizePages(lastHeapFile, fsh.heapMetaSize, fsh.option)

			fsh.lastAddressInAddressSpace += uint64(extraPages)
			pagesRemainingToAllocate -= uint64(extraPages)
		}

	}

	return nil
}

func (fsh *fileSystemHeap) allocatePagesInHeapFile(hpf *heapfilemeta, heapFileSize int64, extraPages int64) error {
	err := syscall.Fallocate(hpf.fd, 0, heapFileSize, extraPages*int64(fsh.option.PageSizeByte))
	if err != nil {
		fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to extend heap file %d", hpf.addressSpaceStart))
		return err
	}
	hpf.pageCount += uint32(extraPages)
	hpf.SerializeMetaData()
	_, err = syscall.Pwrite(hpf.fd, hpf.buffer, 0)
	if err != nil {
		hpf.pageCount -= uint32(extraPages)
		fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to write heap file %d", hpf.addressSpaceStart))
		return err
	}

	err = syscall.Fsync(hpf.fd)
	if err != nil {
		fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to fsync heap file %d", hpf.addressSpaceStart))
		return err
	}

	return nil
}

func (fsh *fileSystemHeap) Read(pageNumber uint64, buffer []byte, onRead func(error)) {

	heapFile := pageNumber / uint64(fsh.maxTotalPagesInHeapFile)
	heapFileOffset := pageNumber % uint64(fsh.maxTotalPagesInHeapFile)

	hpf, ok := fsh.startAddressMap[fsh.fileIdentifiers[heapFile].addressSpaceStart]

	if !ok {
		onRead(errors.New("page not found"))
	}

	_, err := syscall.Pread(hpf.fd, buffer, int64(fsh.heapMetaSize)+int64(heapFileOffset*uint64(fsh.option.PageSizeByte)))

	onRead(err)
}

func (fsh *fileSystemHeap) Write(pageNumber uint64, buffer []byte, onWrite func(error)) {

	heapFile := pageNumber / uint64(fsh.maxTotalPagesInHeapFile)
	heapFileOffset := pageNumber % uint64(fsh.maxTotalPagesInHeapFile)

	hpf, ok := fsh.startAddressMap[fsh.fileIdentifiers[heapFile].addressSpaceStart]

	if !ok {
		onWrite(errors.New("page not found"))
	}

	_, err := syscall.Pwrite(hpf.fd, buffer, int64(fsh.heapMetaSize)+int64(heapFileOffset*uint64(fsh.option.PageSizeByte)))

	if err := syscall.Fsync(hpf.fd); err != nil {
		fsh.logger.Error().Err(err).Msg(fmt.Sprintf("Failed to fsync heap file %d", heapFile))
		onWrite(err)
		return
	}

	onWrite(err)
}

func (fsh *fileSystemHeap) ValidAddressRange() [2]uint64 {
	fsh.heapFileLock.Lock()
	defer fsh.heapFileLock.Unlock()
	return [2]uint64{fsh.firstAddressInAddressSpace, fsh.lastAddressInAddressSpace}
}

/*
Creates heapfile in sequence , starts with a heap file of size page size
if list of heap file is empty. If not loads the heapfile file pointer
and stores them.
*/
func NewHeap(logger log.Logger, option *HeapFileOptions) (HeapFile, error) {

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

			addressSpaceStart, err := strconv.ParseInt(strings.Split(fileEntry.Name(), heapfileNameSepparate)[1], 10, 64)

			if err != nil {
				logger.Error().Err(err).Msg(fmt.Sprintf("Failed to parse heap file number %s", fileEntry.Name()))
				return nil, err
			}

			// stash the fd and heap file number
			hpf := &heapfilemeta{
				fd:                fd,
				addressSpaceStart: uint64(addressSpaceStart),
				options:           option,
			}

			fileIdentifiersMap[fileEntry.Name()] = hpf
		}
	}

	for _, hpf := range fileIdentifiersMap {

		buffer := make([]byte, heapFileMetaSize)
		_, err := syscall.Pread(hpf.fd, buffer, 0)
		if err != nil {
			logger.Error().Err(err).Msg("Failed to read heap file")
			return nil, err
		}
		hpf.buffer = buffer
		stat, err := os.Stat(filepath.Join(option.FileDirectory, heapFileName(hpf.addressSpaceStart)))
		// corrects the file meta based on the file size
		// further correction logic can involve reading all the pages and checking how many of them are legit
		// pages and then truncating them off
		// TODO:
		// potential issue of partial page writes is not handled. So if the heap has bunch of corrupted pages
		// its upto page manager to handle it. or the system can be set in RAID 1
		if hpf.DeserializeMetadat() != nil {

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

			hpf.SerializeMetaData()
			_, err = syscall.Pwrite(hpf.fd, hpf.buffer, 0)
			if err != nil {
				logger.Error().Err(err).Msg("Failed to write heap file")
				return nil, err
			}
			if err := syscall.Fsync(hpf.fd); err != nil {
				logger.Error().Err(err).Msg(fmt.Sprintf("Failed to fsync heap file %d", hpf.addressSpaceStart))
				return nil, err
			}
			hpf.DeserializeMetadat()
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

		// meta space ignoring
		createFreeSizePages(hpf, heapFileMetaSize, option)
		fileIdentifiers = append(fileIdentifiers, hpf)
	}

	sort.Slice(fileIdentifiers, func(i, j int) bool {
		return fileIdentifiers[i].addressSpaceStart < fileIdentifiers[j].addressSpaceStart
	})

	if len(fileIdentifiers) == 0 {
		hpm, err := createNewEmptyHeapFile(0, option, logger)
		if err != nil {
			return nil, err
		}

		// TODO : correct file size based on meta
		fileIdentifiers = append(fileIdentifiers, hpm)
	}

	startAddressMap := make(map[uint64]*heapfilemeta, len(fileIdentifiers))

	for _, hpf := range fileIdentifiers {
		startAddressMap[hpf.addressSpaceStart] = hpf
	}

	return &fileSystemHeap{
		logger:                     logger,
		fileIdentifiers:            fileIdentifiers,
		firstAddressInAddressSpace: fileIdentifiers[0].addressSpaceStart,
		lastAddressInAddressSpace:  fileIdentifiers[len(fileIdentifiers)-1].addressSpaceStart + uint64(fileIdentifiers[len(fileIdentifiers)-1].pageCount) - 1,
		maxTotalPagesInHeapFile:    totalPagesInHeapFile(option),
		heapMetaSize:               heapFileMetaSize,
		heapFileLock:               &sync.RWMutex{},
		startAddressMap:            startAddressMap,
		option:                     option,
	}, nil
}

func createFreeSizePages(hpf *heapfilemeta, heapFileMetaSize uint32, option *HeapFileOptions) {
	hpf.freelist = make([]freelist.FreeList, 0, heapFileMetaSize/option.PageSizeByte)
	numFreeListPages := int((heapFileMetaSize - option.PageSizeByte) / option.PageSizeByte)
	for i := 0; i < numFreeListPages; i++ {
		buffer := hpf.buffer[option.PageSizeByte:]
		hpf.freelist = append(hpf.freelist, freelist.NewBitmapFreeList(buffer[i*int(option.PageSizeByte):(i+1)*int(option.PageSizeByte)], 0, uint64(hpf.pageCount)))
	}
}

func createNewEmptyHeapFile(addressSpaceStart uint64, option *HeapFileOptions, logger log.Logger) (*heapfilemeta, error) {

	heapFileMetaSize := getHeapFileMetaSize(option)

	fd, err := syscall.Open(filepath.Join(option.FileDirectory, heapFileName(addressSpaceStart)), syscall.O_RDWR|syscall.O_DSYNC|syscall.O_CREAT, permissionBits)
	if err != nil {
		logger.Error().Err(err).Msg(fmt.Sprintf("Failed to open heap file %d", addressSpaceStart))
		return nil, err
	}

	err = syscall.Fallocate(fd, 0, 0, int64(heapFileMetaSize))
	if err != nil {
		logger.Error().Err(err).Msg(fmt.Sprintf("Failed to allocate page in heap file %d", addressSpaceStart))
		return nil, err
	}

	hpm := &heapfilemeta{
		pageCount:         0,
		fd:                fd,
		addressSpaceStart: addressSpaceStart,
		options:           option,
	}

	hpm.buffer = make([]byte, heapFileMetaSize)
	hpm.SerializeMetaData()

	_, err = syscall.Pwrite(fd, hpm.buffer, 0)

	if err != nil {
		logger.Error().Err(err).Msg(fmt.Sprintf("Failed to write heap file %d", addressSpaceStart))
		return nil, err
	}
	if err := syscall.Fsync(fd); err != nil {
		logger.Error().Err(err).Msg(fmt.Sprintf("Failed to fsync heap file %d", addressSpaceStart))
		return nil, err
	}

	createFreeSizePages(hpm, heapFileMetaSize, option)

	return hpm, nil
}

func heapFileName(number uint64) string {
	return fmt.Sprintf("%s%s%d", heapFileNamePrefix, heapfileNameSepparate, number)
}
