package records

type ColumnarPageRecordsManager struct {
	buffer []byte
}

func (p *ColumnarPageRecordsManager) Flush() []byte {
	return p.buffer
}

func (p *ColumnarPageRecordsManager) GetInstance() PageManager {
	return p
}

func (p *ColumnarPageRecordsManager) AddRecord(key int64, value []byte) error {
	// check for buffer size and then make a safe entry (preferrably zero copy ?)
	return nil
}
