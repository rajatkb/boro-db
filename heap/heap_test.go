package heap

import (
	"boro-db/logging"
	"os"
	"path/filepath"
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

		heapFile.AllocatePage(4)
		assert.Len(t, hpf.fileIdentifiers, 1)
		assert.Equal(t, uint64(4), hpf.totalAddressablePages)
		assert.Equal(t, uint32(4), hpf.fileIdentifiers[0].pageCount)

	})

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
		assert.Equal(t, uint64(4), hpf.totalAddressablePages)
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
		heapFile.AllocatePage(4)
		assert.Len(t, hpf.fileIdentifiers, 2)
		assert.Equal(t, uint64(8), hpf.totalAddressablePages)
		assert.Equal(t, uint32(4), hpf.fileIdentifiers[0].pageCount)
		assert.Equal(t, uint32(4), hpf.fileIdentifiers[1].pageCount)

		heapFile.AllocatePage(1)
		assert.Len(t, hpf.fileIdentifiers, 3)
		assert.Equal(t, uint64(9), hpf.totalAddressablePages)
		assert.Equal(t, uint32(4), hpf.fileIdentifiers[0].pageCount)
		assert.Equal(t, uint32(4), hpf.fileIdentifiers[1].pageCount)
		assert.Equal(t, uint32(1), hpf.fileIdentifiers[2].pageCount)

		heapFile.AllocatePage(10)
		assert.Len(t, hpf.fileIdentifiers, 5)
		assert.Equal(t, uint64(19), hpf.totalAddressablePages)
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
		assert.Equal(t, uint64(19), hpf.totalAddressablePages)
		assert.Equal(t, uint32(4), hpf.fileIdentifiers[0].pageCount)
		assert.Equal(t, uint32(4), hpf.fileIdentifiers[1].pageCount)
		assert.Equal(t, uint32(4), hpf.fileIdentifiers[2].pageCount)
		assert.Equal(t, uint32(4), hpf.fileIdentifiers[3].pageCount)
		assert.Equal(t, uint32(3), hpf.fileIdentifiers[4].pageCount)

		heapFile.Truncate(10)
		assert.Len(t, hpf.fileIdentifiers, 3)
		assert.Equal(t, uint64(10), hpf.totalAddressablePages)
		assert.Equal(t, uint32(4), hpf.fileIdentifiers[0].pageCount)
		assert.Equal(t, uint32(4), hpf.fileIdentifiers[1].pageCount)
		assert.Equal(t, uint32(2), hpf.fileIdentifiers[2].pageCount)

	})

}
