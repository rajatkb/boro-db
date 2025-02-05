package paging

// type DummyTranslator struct {
// 	Data []byte
// }

// func (d *DummyTranslator) GetInstance() *DummyTranslator {
// 	return d
// }

// func (d *DummyTranslator) Flush() []byte {
// 	return d.Data
// }

// func newDummyTranslator(data []byte) TranslatePage[*DummyTranslator] {
// 	return &DummyTranslator{
// 		Data: data,
// 	}
// }

// func TestSimpleLRUPager(t *testing.T) {
// 	logger := logging.CreateDebugLogger()
// 	fileOps := file.NewHeapDiskFileReaderWriter(logger, file.FileOptions{PageSizeByte: file.MIN_PAGE_SIZE, FileDirectory: "./data"})
// 	pager := NewPagingStrategy[*DummyTranslator, *DummyTranslator](newDummyTranslator, fileOps, logger, PagingOptions{
// 		PageStrategy:    SimpleLRUStrategy,
// 		FlushIntervalMs: 10000,
// 	})

// 	pageBuffer0 := GetEmptyPage(0, &DummyTranslator{Data: make([]byte, file.MIN_PAGE_SIZE)})

// 	pageBuffer0.PageData.Flush()[0] = 'h'
// 	pageBuffer0.PageData.Flush()[1] = 'i'
// 	pageBuffer0.PageData.Flush()[2] = '!'
// 	var wg sync.WaitGroup
// 	wg.Add(1)
// 	pager.Write(&pageBuffer0, func(p *Page[*DummyTranslator, *DummyTranslator], err error) {
// 		if err != nil {
// 			t.Error(err, fmt.Sprintf("Failed to write page %v", err))
// 		}
// 		assert.Equal(t, p.PageData, pageBuffer0.PageData)
// 		wg.Done()
// 	})

// 	pageBuffer1 := GetEmptyPage(1, &DummyTranslator{Data: make([]byte, file.MIN_PAGE_SIZE)})
// 	pageBuffer1.PageData.Flush()[0] = 'h'
// 	pageBuffer1.PageData.Flush()[1] = 'e'
// 	pageBuffer1.PageData.Flush()[2] = 'l'
// 	pageBuffer1.PageData.Flush()[3] = 'l'
// 	pageBuffer1.PageData.Flush()[4] = 'o'
// 	wg.Add(1)
// 	pager.Write(&pageBuffer1, func(p *Page[*DummyTranslator, *DummyTranslator], err error) {
// 		if err != nil {
// 			t.Error(err, fmt.Sprintf("Failed to write page %v", err))
// 		}
// 		assert.Equal(t, p.PageData, pageBuffer1.PageData)
// 		wg.Done()
// 	})

// 	wg.Wait()

// 	pager.Flush()

// 	pager.Read(0, func(p *Page[*DummyTranslator, *DummyTranslator], err error) {
// 		if err != nil {
// 			t.Error(err, fmt.Sprintf("Failed to read page %v", err))
// 		}
// 		assert.Equal(t, p.PageData, pageBuffer0.PageData)
// 	})

// 	pager.Read(1, func(p *Page[*DummyTranslator, *DummyTranslator], err error) {
// 		if err != nil {
// 			t.Error(err, fmt.Sprintf("Failed to read page %v", err))
// 		}
// 		assert.Equal(t, p.PageData, pageBuffer1.PageData)
// 	})

// 	os.Remove("./data/temp.dat")
// 	syscall.Rmdir("./data")

// }
