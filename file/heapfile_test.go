package file

// func TestHeapFileInstatiation(t *testing.T) {

// 	pt, _ := os.Getwd()
// 	dir := filepath.Join(pt, "test")

// 	option := FileOptions{
// 		PageSizeByte:     4096,
// 		FileDirectory:    dir,
// 		HeapFileSizeByte: 4161536,
// 		// 508 * 4096byte ,tests for two set of 508 pages per file.
// 		// two files in total
// 	}

// 	logger := logging.CreateDebugLogger()

// 	t.Run("database files creation test", func(t *testing.T) {
// 		fileop := NewHeapDiskFileReaderWriter(logger, option)

// 		if stat, err := os.Stat(dir); err != nil {
// 			t.Error(err)
// 		} else {
// 			assert.True(t, stat.IsDir())
// 			directoryFile := filepath.Join(dir, directoryFilename)
// 			if stat, err := os.Stat(directoryFile); err != nil {
// 				t.Error(err)
// 			} else {
// 				assert.Equal(t, stat.Size(), int64(MIN_PAGE_SIZE*2))
// 			}
// 		}

// 		if stat, err := os.Stat(createHeapFileName(option.FileDirectory, heapFileNamePrefix, 0)); err != nil {
// 			t.Error(err)
// 		} else {
// 			assert.Equal(t, stat.Size(), int64(MIN_PAGE_SIZE*totalPageRefInSingleDirectoryRecord())) // 2.08 Mb
// 		}

// 		ops := fileop.(*diskFileOps)

// 		drb := createNewDirectoryRecordPage(totalPageRefInSingleDirectoryRecord(), option.PageSizeByte, []uint64{0}, []uint64{0})

// 		assert.Equal(t, ops.directoryMeta, directoryMeta{
// 			pageSize:    4096,
// 			maxFileSize: 4161536,
// 			fd:          ops.directoryMeta.fd,
// 			directoryRecords: []*directoryRecord{
// 				{
// 					buffer: drb[:],
// 				},
// 			},
// 		}, "directory records test failed")
// 	})

// 	// rereading the directory file this time its not a new creation

// 	t.Run("database files reading test", func(t *testing.T) {
// 		fileop := NewHeapDiskFileReaderWriter(logger, option)

// 		ops := fileop.(*diskFileOps)

// 		drb := createNewDirectoryRecordPage(totalPageRefInSingleDirectoryRecord(), option.PageSizeByte, []uint64{0}, []uint64{0})

// 		assert.Equal(t, ops.directoryMeta, directoryMeta{
// 			pageSize:    4096,
// 			maxFileSize: 4161536,
// 			fd:          ops.directoryMeta.fd,
// 			directoryRecords: []*directoryRecord{
// 				{
// 					buffer: drb[:],
// 				},
// 			},
// 		}, "failed to read directory records")

// 		assert.Equal(t, uint64(totalPageRefInSingleDirectoryRecord()), fileop.GetPageCount())

// 		assert.Nil(t, fileop.ExpandBy(1))

// 		assert.Len(t, ops.directoryMeta.directoryRecords, 2)

// 		assert.Len(t, ops.heapfIles, 1)
// 	})

// 	// t.Run("database read and write test", func(t *testing.T) {
// 	// 	fileop := NewHeapDiskFileReaderWriter(logger, option)
// 	// 	lastIndex := fileop.GetPageCount() - 1
// 	// 	data := []byte("hello")
// 	// 	pageMetaPrev, err := fileop.GetPageMeta(lastIndex)
// 	// 	assert.Nil(t, err)
// 	// 	page, err := fileop.ReadSync(lastIndex)
// 	// 	assert.Nil(t, err)
// 	// 	page.SetPageBuffer(data)

// 	// 	assert.Nil(t, fileop.WriteSync(lastIndex, page))
// 	// 	pageMeta, err := fileop.GetPageMeta(lastIndex)
// 	// 	assert.Nil(t, err)
// 	// 	assert.Equal(t, pageMetaPrev.PageFreeSize-uint64(len(data)), pageMeta.PageFreeSize)

// 	// })

// 	os.RemoveAll(dir)
// }
