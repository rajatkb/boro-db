package records

import "boro-db/paging"

/*
 A page record has following data
 - keys - []int64 = [][4]byte
 - values - [][]byte

*/

type PageManagerTranslator interface {
	GetInstance() PageManager
	Flush() []byte
}

type PageManager interface {
	AddRecord(key int64, value []byte) error
}

func NewPageRecordsManager(buffer []byte) paging.TranslatePage[PageManager] {
	return &ColumnarPageRecordsManager{buffer: buffer}
}
