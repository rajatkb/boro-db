package storage

import (
	"boro-db/heap"
	"boro-db/paging"

	"github.com/phuslu/log"
)

type KeyType int

const (
	Int64 KeyType = iota
	Int32
	Int16
	Int8
	Float64
	VARCHAR
)

type lsmstorage struct {
	keyType KeyType
	logger  log.Logger
	heap    heap.HeapFile
	paging  paging.PageSystem
}

type KVStore interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
}
