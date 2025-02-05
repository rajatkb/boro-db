package file

/*
- This file contains the FileOptions struct and the FileOperation interface.
- this manages the page file writing and reading
-

Directory Structure and Page map for a single page file
PageID = directoryPageNumber * pageSize (4096) + entryOffset
PageID ranges from 0 - MAX_UNSIGNED_INT
┌──────────────────────────────────────────────────────────────┐
| ┌──────────────────────4kb─────────────────────────────────┐ |
| | [dir meta , files * 4kb page , 168 page ref per page]    | |
| | pageSize (8byte) | fileSize 8byte -- padding till 4kb    | |
| └──────────────────────────────────────────────────────────┘ |
|---------------free page list --------------------------------|
| ┌──────────────────────4kb─────────────────────────────────┐ |
| |────Directory Record Page ────────────────────────────────| |
| └──────────────────────────────────────────────────────────┘ |
| -- repeat director data x (max file size / 169 * pageSize ) -|
└──────────────────────────────────────────────────────────────┘



- We create directory and pageFile sepparately
- a directory can grow infinitely till disk space
- a page file will have max growth size of default 4TB
- each page will have checksum + pageID + pageSize + empty end byte
  - this ensures in case the directory got corrupted we can still recover
*/

// const permissionBits = 0755 // directory requires executioin as well hence 7 bit
// const directoryFilename = "directory.dat"
// const heapFileNamePrefix = "heapFile"
// const heapfileNameSepparate = "-"

// type directoryMeta struct {
// 	pageSize         uint64
// 	maxFileSize      uint64
// 	fd               int
// 	directoryRecords []*directoryRecord
// 	// more space for extra metadata
// }

// type heapFile struct {
// 	fullPath string
// 	fd       int
// 	size     uint64
// }

// type diskFileOps struct {
// 	directoryFilePath string
// 	directoryMeta     directoryMeta
// 	heapfIles         map[int]*heapFile
// 	option            FileOptions
// 	logger            *log.Logger
// }

// func (dop *diskFileOps) Read(pageID uint64, readCompleteCallback func(pageData *PageFileBlock, err error)) {
// 	totalRecordInDirectortRecord := totalPageRefInSingleDirectoryRecord()

// 	recordOffset := pageID % uint64(totalRecordInDirectortRecord)
// 	recordIndex := pageID / uint64(totalRecordInDirectortRecord)

// 	if len(dop.directoryMeta.directoryRecords) <= int(recordIndex) {
// 		readCompleteCallback(nil, fmt.Errorf("pageID out of range"))
// 		return
// 	}

// 	pageRef := dop.directoryMeta.directoryRecords[int(recordIndex)].readPageRef(int(recordOffset))

// 	fd := dop.heapfIles[int(pageRef.fileID)].fd

// 	buffer := make([]byte, dop.directoryMeta.pageSize)

// 	_, err := syscall.Pread(fd, buffer, int64(pageRef.offset)+int64(recordOffset*dop.directoryMeta.pageSize))

// 	pfb := readHeapPage(buffer)

// 	readCompleteCallback(pfb, err)
// 	return
// }

// func (dop *diskFileOps) Write(pageID uint64, data *PageFileBlock, writeCompleteCallback func(error)) {

// 	totalRecordInDirectortRecord := totalPageRefInSingleDirectoryRecord()

// 	recordOffset := pageID % uint64(totalRecordInDirectortRecord)
// 	recordIndex := pageID / uint64(totalRecordInDirectortRecord)

// 	if len(dop.directoryMeta.directoryRecords) <= int(recordIndex) {
// 		writeCompleteCallback(fmt.Errorf("pageID out of range"))
// 		return
// 	}

// 	record := dop.directoryMeta.directoryRecords[int(recordIndex)]
// 	pageRef := record.readPageRef(int(recordOffset))

// 	// TODO: use WAL
// 	fd := dop.heapfIles[int(pageRef.fileID)].fd
// 	_, err := syscall.Pwrite(fd, data.Serialize(), int64(pageRef.offset)+int64(recordIndex*dop.directoryMeta.pageSize))

// 	if err != nil {
// 		writeCompleteCallback(err)
// 		return
// 	}

// 	// TODO: use WAL
// 	_, err = syscall.Pwrite(dop.directoryMeta.fd, record.Serialize(), int64(MIN_PAGE_SIZE)+int64(recordIndex)*int64(MIN_PAGE_SIZE))

// 	if err != nil {
// 		writeCompleteCallback(err)
// 		return
// 	}

// 	writeCompleteCallback(nil)
// }
// func (dop *diskFileOps) recordsPerHeapFile() int {
// 	return int(math.Ceil(float64(dop.option.HeapFileSizeByte) / float64(dop.directoryMeta.pageSize*uint64(totalPageRefInSingleDirectoryRecord()))))
// }

// func (dop *diskFileOps) heapPageTotalSize(recordCount uint64) uint64 {
// 	return recordCount * uint64(totalPageRefInSingleDirectoryRecord()) * dop.directoryMeta.pageSize
// }

// func (dop *diskFileOps) ExpandBy(pageCount int) error {

// 	// dop.mutex.Lock()
// 	// defer dop.mutex.Unlock()

// 	// directoryRecordsNeededForPages := int(math.Ceil(float64(pageCount) / float64(totalPageRefInSingleDirectoryRecord())))
// 	// recordsPerHeapFile := dop.recordsPerHeapFile()

// 	// requiredSpaceInBytes := dop.heapfIles[len(dop.heapfIles)-1].size + uint64(directoryRecordsNeededForPages)*dop.directoryMeta.pageSize
// 	// newHeapFileNeeded := false
// 	// lastHeapFileRecordsCount := 0
// 	// totalNewFullSizeHeapFilesToCreate := 0
// 	// if requiredSpaceInBytes > dop.option.HeapFileSizeByte {
// 	// 	newHeapFileNeeded = true
// 	// 	lastHeapFileRecordsCount = int(math.Ceil(float64((requiredSpaceInBytes%dop.option.HeapFileSizeByte)-dop.option.HeapFileSizeByte) / float64(dop.option.PageSizeByte*uint64(totalPageRefInSingleDirectoryRecord()))))
// 	// 	totalNewFullSizeHeapFilesToCreate = int(math.Floor(float64(requiredSpaceInBytes) / float64(dop.option.HeapFileSizeByte)))
// 	// }
// 	// recordsNeededToAddToExistingLastHeapFile := (float64(dop.option.HeapFileSizeByte) - float64(dop.heapfIles[len(dop.heapfIles)-1].size)) / float64(dop.directoryMeta.pageSize*uint64(totalPageRefInSingleDirectoryRecord()))
// 	// recordsCountToAddToExistingLastHeapFile := int(math.Ceil(math.Min(recordsNeededToAddToExistingLastHeapFile, float64(directoryRecordsNeededForPages))))

// 	// if directoryRecordsNeededForPages != lastHeapFileRecordsCount+recordsCountToAddToExistingLastHeapFile {
// 	// 	return fmt.Errorf("failed to expand directory records , floating point issue in calculating record counts needed")
// 	// }

// 	// // creating entries in current heap file
// 	// {
// 	// 	// created new heap file and expand directory record
// 	// 	lastDirRecord := dop.directoryMeta.directoryRecords[len(dop.directoryMeta.directoryRecords)-1]

// 	// 	fileID := int(lastDirRecord.GetPageFileID())

// 	// 	heapFile := dop.heapfIles[fileID]

// 	// 	if err := syscall.Fallocate(heapFile.fd, 0, int64(heapFile.size), int64(uint64(totalPageRefInSingleDirectoryRecord())*dop.directoryMeta.pageSize)*int64(recordsCountToAddToExistingLastHeapFile)); err != nil {
// 	// 		dop.logger.Err(err).Msg("failed to allocate space for new directory record page")
// 	// 		return err
// 	// 	}
// 	// 	prevSize := heapFile.size

// 	// 	// create empty page file in heap
// 	// 	emptyHeapPages := createEmptyPages(totalPageRefInSingleDirectoryRecord()*recordsCountToAddToExistingLastHeapFile, dop.option)

// 	// 	_, err := syscall.Pwrite(heapFile.fd, emptyHeapPages, int64(prevSize))
// 	// 	if err != nil {
// 	// 		dop.logger.Err(err).Msg(fmt.Sprintf("unable to write to heap file : %s", heapFile.fullPath))
// 	// 		return err
// 	// 	}

// 	// 	startOffsets := make([]uint64, recordsCountToAddToExistingLastHeapFile)
// 	// 	for i := 0; i < recordsCountToAddToExistingLastHeapFile; i++ {
// 	// 		startOffsets[i] = uint64(prevSize) + uint64(i*totalPageRefInSingleDirectoryRecord())*dop.directoryMeta.pageSize
// 	// 	}

// 	// 	directoryPage := createNewDirectoryRecordPage(totalPageRefInSingleDirectoryRecord(), dop.option.PageSizeByte, uint64(fileID), startOffsets)

// 	// 	_, err = syscall.Pwrite(dop.directoryMeta.fd, directoryPage[:], int64(len(dop.directoryMeta.directoryRecords)*MIN_PAGE_SIZE+MIN_PAGE_SIZE))

// 	// 	if err != nil {
// 	// 		dop.logger.Err(err).Msg("failed to append directory record page")
// 	// 		return err
// 	// 	}

// 	// 	heapFile.size += uint64(totalPageRefInSingleDirectoryRecord()*recordsCountToAddToExistingLastHeapFile) * dop.directoryMeta.pageSize

// 	// 	dop.directoryMeta.directoryRecords = append(dop.directoryMeta.directoryRecords, &directoryRecord{
// 	// 		fileID: uint64(fileID),
// 	// 		dirty:  true,
// 	// 		buffer: directoryPage[:],
// 	// 	})
// 	// }

// 	// if newHeapFileNeeded {
// 	// 	// created new heap file and expand directory record
// 	// 	newHeapFileId := len(dop.heapfIles) + 1

// 	// 	// we try creating the heap file.
// 	// 	// because the directory records are expanded only when heap size increase or when heap file is created
// 	// 	// we can ensure that
// 	// 	hp, err := createHeapFileWithPagesForDirectoryRecords(dop.option, dop.logger, newHeapFileId, 1)

// 	// 	if err != nil {
// 	// 		dop.logger.Err(err).Msg("failed to create new heap file during page space expansion")
// 	// 		return err
// 	// 	}

// 	// 	dop.heapfIles[newHeapFileId] = hp

// 	// 	entryBytes := createNewDirectoryRecordPage(totalPageRefInSingleDirectoryRecord(), dop.option.PageSizeByte, uint64(newHeapFileId), []uint64{0})

// 	// 	if err := syscall.Fallocate(dop.directoryMeta.fd, 0, int64(len(dop.directoryMeta.directoryRecords)*MIN_PAGE_SIZE+MIN_PAGE_SIZE), MIN_PAGE_SIZE); err != nil {
// 	// 		dop.logger.Err(err).Msg("failed to allocate space for new directory record page")
// 	// 		return err
// 	// 	}

// 	// 	if _, err := syscall.Pwrite(dop.directoryMeta.fd, entryBytes[:], int64(len(dop.directoryMeta.directoryRecords)*MIN_PAGE_SIZE+MIN_PAGE_SIZE)); err != nil {
// 	// 		dop.logger.Err(err).Msg("failed to write directory record page")
// 	// 		return err
// 	// 	}

// 	// 	dop.directoryMeta.directoryRecords = append(dop.directoryMeta.directoryRecords, &directoryRecord{
// 	// 		fileID: uint64(newHeapFileId),
// 	// 		dirty:  true,
// 	// 		buffer: entryBytes[:],
// 	// 	})

// 	// }

// 	return nil
// }

// func (dop *diskFileOps) Free(pageID uint64) {

// }

// func (dop *diskFileOps) Next(pageID uint64) (uint64, error) {
// 	return 0, nil
// }

// func (dop *diskFileOps) GetPageCount() uint64 {

// 	return uint64(totalPageRefInSingleDirectoryRecord()) * uint64(len(dop.directoryMeta.directoryRecords))
// }

// func (dop *diskFileOps) GetPageMeta(pageID uint64) (*PageMeta, error) {

// 	totalRecordInDirectortRecord := totalPageRefInSingleDirectoryRecord()

// 	recordOffset := pageID % uint64(totalRecordInDirectortRecord)
// 	recordIndex := pageID / uint64(totalRecordInDirectortRecord)

// 	if len(dop.directoryMeta.directoryRecords) <= int(recordIndex) {
// 		return nil, fmt.Errorf("pageID out of range")
// 	}

// 	record := dop.directoryMeta.directoryRecords[int(recordIndex)].readPageRef(int(recordOffset))

// 	return &PageMeta{
// 		RecordIndex:   recordIndex,
// 		RecordsOffset: recordOffset,
// 		FileID:        record.fileID,
// 		PageFreeBytes: record.freeBytes,
// 		FileOffset:    record.offset,
// 	}, nil
// }

// func (dop *diskFileOps) Close() {
// 	// syscall.Close(dop.fd)
// }

// func readDirectorMetaAndCreateIfNotExists(logger *log.Logger, newFile bool, option FileOptions) directoryMeta {

// 	directoryPath := filepath.Join(option.FileDirectory, directoryFilename)

// 	if newFile {
// 		// create the directory meta + create space for directory data
// 		// fresh file
// 		fd, err := syscall.Open(directoryPath, syscall.O_DSYNC|syscall.O_CREAT|syscall.O_RDWR, permissionBits)
// 		if err != nil {
// 			logger.Err(err).Msg(fmt.Sprintf("unable to create directory file : %s", directoryPath))
// 			panic(fmt.Sprintf("unable to create directory file : %s", directoryPath))
// 		}
// 		buffer := createDirectoryMeta(option)

// 		if _, err := syscall.Pwrite(fd, buffer, 0); err != nil {
// 			logger.Error().Msg("unable to wrtite to director")
// 			panic("unable to write to directory")
// 		}

// 		totalRecords := totalPageRefInSingleDirectoryRecord()

// 		if err := syscall.Fallocate(fd, 0, MIN_PAGE_SIZE, MIN_PAGE_SIZE); err != nil {
// 			logger.Err(err).Msg("unable to allocate space for directory")
// 			panic("unable to allocate space for directory")
// 		}

// 		// create a single page in the heap
// 		_, err = createHeapFileWithPagesForDirectoryRecords(option, logger, 0, 1)

// 		if err != nil {
// 			logger.Err(err).Msg("unable to allocate space for heap file")
// 			panic("unable to allocate space for heap file")
// 		}

// 		// creating new directory record for the empty heap file
// 		drb := createNewDirectoryRecordPage(totalRecords, option.PageSizeByte, 0, []uint64{0})

// 		_, err = syscall.Pwrite(fd, drb[:], MIN_PAGE_SIZE)
// 		if err != nil {
// 			logger.Err(err).Msg("unable to write directory record data")
// 			panic("unable to write directory data")
// 		}

// 		return directoryMeta{
// 			pageSize:    option.PageSizeByte,
// 			maxFileSize: option.HeapFileSizeByte,
// 			fd:          fd,
// 			directoryRecords: []*directoryRecord{
// 				{
// 					fileID: 0,
// 					buffer: drb[:],
// 				},
// 			},
// 		}
// 	}

// 	directory, err := os.ReadFile(directoryPath)
// 	if err != nil {
// 		logger.Err(err).Msg(fmt.Sprintf("unable to read directory file : %s", directoryPath))
// 		panic(fmt.Sprintf("unable to read directory file : %s", directoryPath))
// 	}

// 	fd, _ := syscall.Open(directoryPath, syscall.O_DSYNC|syscall.O_RDWR, permissionBits)

// 	dm, err := readDirectortMeta(directory[0:MIN_PAGE_SIZE], option)

// 	if err != nil {
// 		logger.Err(err).Msg("unable to read directory meta data. failed to start file system")
// 		panic("unable to read directory meta data. failed to start file system")
// 	}

// 	dm.fd = fd
// 	for i := MIN_PAGE_SIZE; i < len(directory); i += MIN_PAGE_SIZE {
// 		dm.directoryRecords = append(dm.directoryRecords, readDirectoryRecordPage(directory[i:i+MIN_PAGE_SIZE]))
// 	}

// 	return dm
// }

// func createDirectoryMeta(option FileOptions) []byte {
// 	buffer := make([]byte, 0, MIN_PAGE_SIZE)
// 	buffer = binary.BigEndian.AppendUint64(buffer, option.PageSizeByte)
// 	buffer = binary.BigEndian.AppendUint64(buffer, option.HeapFileSizeByte)
// 	return buffer
// }

// func readDirectortMeta(buffer []byte, option FileOptions) (directoryMeta, error) {
// 	pageSize := binary.BigEndian.Uint64(buffer[:8])
// 	fileSize := binary.BigEndian.Uint64(buffer[8:16])

// 	if option.PageSizeByte != pageSize || option.HeapFileSizeByte != fileSize {
// 		return directoryMeta{}, fmt.Errorf("directory meta data is not matching with the file options. meta values:  pageSize : %d fileSize : %d", pageSize, fileSize)
// 	}
// 	return directoryMeta{
// 		pageSize:    pageSize,
// 		maxFileSize: fileSize,
// 	}, nil
// }

// func createHeapFileName(fileDir string, heapFileNamePrefix string, fileID int) string {
// 	return filepath.Join(fileDir, strings.Join([]string{heapFileNamePrefix, strconv.FormatInt(int64(fileID), 10)}, heapfileNameSepparate))
// }

// // createHeapFileWithPagesForDirectoryRecords creates a new empty heap file and extends its space to totalDirectoryRecord * pageSize
// func createHeapFileWithPagesForDirectoryRecords(option FileOptions, logger *log.Logger, fileID int, numberOfRecords int) (*heapFile, error) {

// 	totalRecords := totalPageRefInSingleDirectoryRecord() * numberOfRecords

// 	heapFilePath := createHeapFileName(option.FileDirectory, heapFileNamePrefix, fileID)

// 	hfd, err := syscall.Open(heapFilePath, syscall.O_RDWR|syscall.O_CREAT|syscall.O_DSYNC, permissionBits)
// 	if err != nil {
// 		logger.Err(err).Msg(fmt.Sprintf("unable to create heap file : %s", heapFilePath))
// 		return nil, err
// 	}

// 	err = syscall.Fallocate(hfd, 0, 0, int64(totalRecords*int(option.PageSizeByte)))

// 	if err != nil {
// 		logger.Err(err).Msg("unable to allocate space for heap file")
// 		return nil, err
// 	}

// 	// create empty page file in heap
// 	emptyHeapPage := createEmptyPages(totalRecords, option)

// 	_, err = syscall.Pwrite(hfd, emptyHeapPage, 0)
// 	if err != nil {
// 		logger.Err(err).Msg(fmt.Sprintf("unable to write to heap file : %s", heapFilePath))
// 		return nil, err
// 	}
// 	return &heapFile{
// 		fullPath: heapFilePath,
// 		fd:       hfd,
// 		size:     uint64(totalRecords * int(option.PageSizeByte)),
// 	}, err
// }

// func createEmptyPages(totalRecords int, option FileOptions) []byte {
// 	emptyHeapPage := make([]byte, totalRecords*int(option.PageSizeByte))

// 	for i := 0; i < totalRecords; i++ {

// 		createEmptyHeapPage(emptyHeapPage[i*int(option.PageSizeByte):i*int(option.PageSizeByte)+int(option.PageSizeByte)], option.PageSizeByte)
// 	}
// 	return emptyHeapPage
// }

// func createLockFile(option FileOptions) error {
// 	lockFile := filepath.Join(option.FileDirectory, "lock")
// 	_, err := syscall.Open(lockFile, syscall.O_CREAT|syscall.O_EXCL|syscall.O_WRONLY, 0600)
// 	return err
// }

// func unlockFile(option FileOptions) error {
// 	lockFile := filepath.Join(option.FileDirectory, "lock")
// 	return os.RemoveAll(lockFile)
// }

// func NewHeapDiskFileReaderWriter(logger *log.Logger, option FileOptions) FileOperation {

// 	if option.HeapFileSizeByte > MAX_HEAP_FILE_SIZE {
// 		logger.Err(fmt.Errorf("max heap file size is too large. max heap file size is %d bytes", MAX_HEAP_FILE_SIZE)).Msg("max heap file size is too large")
// 		panic("max heap file size is too large")
// 	}

// 	if entry, err := os.Stat(option.FileDirectory); err != nil {
// 		if err := syscall.Mkdir(option.FileDirectory, permissionBits); err != nil {
// 			logger.Err(err).Msg(fmt.Sprintf("unable to create directory : %s", option.FileDirectory))
// 			panic(fmt.Sprintf("unable to create directory : %s", option.FileDirectory))
// 		}
// 	} else if !entry.IsDir() {
// 		panic(fmt.Sprintf("file %s is not a directory", option.FileDirectory))
// 	}

// 	// create a lock file here (if the lock file is present it means during file creation we crashed)
// 	if err := createLockFile(option); err != nil {
// 		logger.Err(err).Msg("unable to create lock file")
// 		panic("unable to create lock file")
// 	}

// 	directoryPath := filepath.Join(option.FileDirectory, directoryFilename)

// 	_, err := os.Stat(directoryPath)

// 	directoryMeta := readDirectorMetaAndCreateIfNotExists(logger, err != nil, option)

// 	var heapfiles map[int]*heapFile
// 	if entries, err := os.ReadDir(option.FileDirectory); err != nil {
// 		logger.Err(err).Msg("unable to read page directory")
// 		panic(err)
// 	} else {

// 		heapfiles = make(map[int]*heapFile)
// 		for _, entry := range entries {

// 			if strings.Contains(entry.Name(), heapFileNamePrefix) {

// 				// collect the file descriptors as well

// 				splits := strings.Split(entry.Name(), heapfileNameSepparate)
// 				pos, err := strconv.ParseInt(splits[1], 10, 64)
// 				if err != nil {
// 					logger.Err(err).Msg(fmt.Sprintf("unable to parse heap file name : %s", entry.Name()))
// 					panic(fmt.Sprintf("unable to parse heap file name : %s", entry.Name()))
// 				}

// 				stat, err := os.Stat(filepath.Join(option.FileDirectory, entry.Name()))
// 				if err != nil {
// 					logger.Err(err).Msg(fmt.Sprintf("unable to get heap file size : %s", entry.Name()))
// 					panic(fmt.Sprintf("unable to get heap file size : %s", entry.Name()))
// 				}

// 				fullPath := filepath.Join(option.FileDirectory, entry.Name())

// 				fd, err := syscall.Open(fullPath, syscall.O_RDWR|syscall.O_DSYNC, permissionBits)

// 				if err != nil {
// 					logger.Err(err).Msg(fmt.Sprintf("unable to open heap file : %s", entry.Name()))
// 					panic(fmt.Sprintf("unable to open heap file : %s", entry.Name()))
// 				}

// 				// TODO detect empty heap file warning and clean them up or panic
// 				// why can there be empty heap files ?
// 				// because disk can fail writing into the heap file post its first creation
// 				// include these heap files in the integrity check list
// 				// and don't include them in this list.
// 				// any heap file which got created must have a corresponding directory record
// 				// heap file is the source of truth , directory record is only a look up table for same
// 				// directory record can be created from a heap file

// 				heapfiles[int(pos)] = &heapFile{
// 					fd:       fd,
// 					size:     uint64(stat.Size()),
// 					fullPath: fullPath,
// 				}
// 			}
// 		}

// 	}

// 	unlockFile(option)

// 	return &diskFileOps{
// 		directoryFilePath: directoryPath,
// 		directoryMeta:     directoryMeta,
// 		heapfIles:         heapfiles,
// 		option:            option,
// 		logger:            logger,
// 	}
// }
