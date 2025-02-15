package cache

type Cache[K any, V any] interface {
	Get(K) (V, bool)
	Put(K, V)
	Evict(K, func(V) bool) bool
	Compact(func(K, V) bool)
	Range(func(K, V) bool)
	Size() int
}
