package cache

// dumb LRU implementation
func NewLRUCache[K comparable, V any](size int) Cache[K, V] {
	return &LRUCache[K, V]{
		cache:    make(map[K]*Node[K, V], size),
		size:     size,
		listHead: nil,
	}
}

type LRUCache[K comparable, V any] struct {
	cache    map[K]*Node[K, V]
	size     int
	listHead *Node[K, V]
	length   int
}

type Node[K comparable, V any] struct {
	key   K
	value V
	prev  *Node[K, V]
	next  *Node[K, V]
}

func (c *LRUCache[K, V]) Size() int {

	return c.length
}

// Range holds a global lock on the page cache
// as long as the lock is held nothing can read
// or write on the cache. Stalling most operations
func (c *LRUCache[K, V]) Range(onEach func(K, V) bool) {

	head := c.listHead
	// while we do range we can also compact the cache
	c.Compact(func(k K, v V) bool {
		return onEach(k, v)
	})
	// post compaction just iterate as usual
	for {
		if !onEach(head.key, head.value) {
			break
		}
		head = head.next
		if head == c.listHead {
			break
		}

	}
}

// Compact holds a global lock on the page cache
// as long as the lock is held nothing can read
// or write on the cache. Stalling most operations
func (c *LRUCache[K, V]) Compact(onEvict func(K, V) bool) {

	if c.length <= c.size {
		return
	}

	for c.length > c.size {
		key := c.listHead.key
		if !c.Evict(key, func(v V) bool {
			return onEvict(key, v)
		}) {
			return
		}
	}
}

// Evict holds a global lock and deletes a page
func (c *LRUCache[K, V]) Evict(key K, preEvict func(V) bool) bool {

	value := c.cache[key]

	if !preEvict(value.value) {
		return false
	}

	delete(c.cache, key)

	value.prev.next = value.next
	value.next.prev = value.prev

	if c.listHead == value {
		c.listHead = value.next
	}

	if c.length == 1 {
		c.listHead = nil
	}

	c.length--
	return true
}

// Put holds a global lock and adds a value to the cache
func (c *LRUCache[K, V]) Put(key K, value V) {

	node := &Node[K, V]{
		key:   key,
		value: value,
	}

	node.prev = node
	node.next = node

	c.cache[key] = node

	if c.listHead != nil {
		c.listHead.prev.next = node
		node.prev = c.listHead.prev
		c.listHead.prev = node
		node.next = c.listHead
	}

	c.listHead = node
	c.length++

}

// Get holds a global lock and returns a value from the cache
func (c *LRUCache[K, V]) Get(key K) (V, bool) {

	value, ok := c.cache[key]

	if !ok {
		return value.value, false
	}

	value.prev.next = value.next
	value.next.prev = value.prev

	c.listHead.prev.next = value
	value.prev = c.listHead.prev
	c.listHead.prev = value
	value.next = c.listHead

	return value.value, true
}
