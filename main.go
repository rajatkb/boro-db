package main

const PAGE_SIZE = 4096 // 4KB

func main() {
	// logger := logging.CreateDebugLogger()
	// fileOps := file.NewHeapDiskFileReaderWriter(logger, file.FileOptions{PageSizeByte: PAGE_SIZE, FileDirectory: "./data"})
	// pager := paging.NewPagingStrategy[records.PageManager, records.PageManagerTranslator](records.NewPageRecordsManager, fileOps, logger, paging.PagingOptions{
	// 	PageStrategy: paging.SimpleLRUStrategy,
	// })

	// defer pager.Flush()
	// defer fileOps.Close()

}
