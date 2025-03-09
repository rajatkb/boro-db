package heap

import (
	"boro-db/logging"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHeapFileOperations(t *testing.T) {

	pt, _ := os.Getwd()
	dir := filepath.Join(pt, "test")

	defer func() {
		os.RemoveAll(dir)
	}()

	t.Run("Test heap file creation", func(t *testing.T) {

		heapFile, err := NewHeap(*logging.CreateDebugLogger(), FileOptions{
			PageSizeByte:     4096,
			FileDirectory:    dir,
			HeapFileSizeByte: 4096 * 5, // 4 page + 1 meta
		})

		assert.Nil(t, err)
		hpf := heapFile.(*fileSystemHeap)

		heapFile.ExtendBy(4)
		assert.Len(t, hpf.fileIdentifiers, 1)
		assert.Equal(t, uint64(3), hpf.lastAddressInAddressSpace)
		assert.Equal(t, uint32(4), hpf.fileIdentifiers[0].pageCount)

		t.Run("Test by reloading the file system", func(t *testing.T) {
			// reload the same file system
			heapFile, err := NewHeap(*logging.CreateDebugLogger(), FileOptions{
				PageSizeByte:     4096,
				FileDirectory:    dir,
				HeapFileSizeByte: 4096 * 5, // 4 page + 1 meta
			})

			assert.Nil(t, err)
			hpf := heapFile.(*fileSystemHeap)
			assert.Len(t, hpf.fileIdentifiers, 1)
			assert.Equal(t, uint64(3), hpf.lastAddressInAddressSpace)
			assert.Equal(t, uint32(4), hpf.fileIdentifiers[0].pageCount)

		})

		t.Run("Test for allocating more page space", func(t *testing.T) {

			// reload the same file system
			heapFile, err := NewHeap(*logging.CreateDebugLogger(), FileOptions{
				PageSizeByte:     4096,
				FileDirectory:    dir,
				HeapFileSizeByte: 4096 * 5, // 4 page + 1 meta
			})

			assert.Nil(t, err)
			hpf := heapFile.(*fileSystemHeap)
			heapFile.ExtendBy(4)
			assert.Len(t, hpf.fileIdentifiers, 2)
			assert.Equal(t, uint64(7), hpf.lastAddressInAddressSpace)
			assert.Equal(t, uint32(4), hpf.fileIdentifiers[0].pageCount)
			assert.Equal(t, uint32(4), hpf.fileIdentifiers[1].pageCount)

			heapFile.ExtendBy(1)
			assert.Len(t, hpf.fileIdentifiers, 3)
			assert.Equal(t, uint64(8), hpf.lastAddressInAddressSpace)
			assert.Equal(t, uint32(4), hpf.fileIdentifiers[0].pageCount)
			assert.Equal(t, uint32(4), hpf.fileIdentifiers[1].pageCount)
			assert.Equal(t, uint32(1), hpf.fileIdentifiers[2].pageCount)

			heapFile.ExtendBy(10)
			assert.Len(t, hpf.fileIdentifiers, 5)
			assert.Equal(t, uint64(18), hpf.lastAddressInAddressSpace)
			assert.Equal(t, uint32(4), hpf.fileIdentifiers[0].pageCount)
			assert.Equal(t, uint32(4), hpf.fileIdentifiers[1].pageCount)
			assert.Equal(t, uint32(4), hpf.fileIdentifiers[2].pageCount)
			assert.Equal(t, uint32(4), hpf.fileIdentifiers[3].pageCount)
			assert.Equal(t, uint32(3), hpf.fileIdentifiers[4].pageCount)

		})

		t.Run("Test for truncation", func(t *testing.T) {

			// reload the same file system
			heapFile, err := NewHeap(*logging.CreateDebugLogger(), FileOptions{
				PageSizeByte:     4096,
				FileDirectory:    dir,
				HeapFileSizeByte: 4096 * 5, // 4 page + 1 meta
			})

			assert.Nil(t, err)
			hpf := heapFile.(*fileSystemHeap)

			assert.Len(t, hpf.fileIdentifiers, 5)
			assert.Equal(t, uint64(18), hpf.lastAddressInAddressSpace)
			assert.Equal(t, uint32(4), hpf.fileIdentifiers[0].pageCount)
			assert.Equal(t, uint32(4), hpf.fileIdentifiers[1].pageCount)
			assert.Equal(t, uint32(4), hpf.fileIdentifiers[2].pageCount)
			assert.Equal(t, uint32(4), hpf.fileIdentifiers[3].pageCount)
			assert.Equal(t, uint32(3), hpf.fileIdentifiers[4].pageCount)

			heapFile.TrimTail(10)
			assert.Len(t, hpf.fileIdentifiers, 3)
			assert.Equal(t, uint64(8), hpf.lastAddressInAddressSpace)
			assert.Equal(t, uint32(4), hpf.fileIdentifiers[0].pageCount)
			assert.Equal(t, uint32(4), hpf.fileIdentifiers[1].pageCount)
			assert.Equal(t, uint32(1), hpf.fileIdentifiers[2].pageCount)

		})

		t.Run("Test for reading and writing", func(t *testing.T) {
			// reload the same file system
			heapFile, err := NewHeap(*logging.CreateDebugLogger(), FileOptions{
				PageSizeByte:     4096,
				FileDirectory:    dir,
				HeapFileSizeByte: 4096 * 5, // 4 page + 1 meta
			})

			assert.Nil(t, err)

			var wg sync.WaitGroup
			wg.Add(1)
			heapFile.Write(4, []byte("Hello World"), func(err error) {
				assert.Nil(t, err)
				wg.Done()
			})
			wg.Wait()

			data := make([]byte, 11)
			wg.Add(1)
			heapFile.Read(4, data, func(err error) {
				assert.Nil(t, err)
				assert.Equal(t, "Hello World", string(data))
				wg.Done()
			})
			wg.Wait()

		})
	})

}
