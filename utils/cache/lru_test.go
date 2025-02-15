package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLRUCache(t *testing.T) {
	c := NewLRUCache[int, int](10)
	cache := c.(*LRUCache[int, int])
	for i := 0; i < 10; i++ {
		cache.Put(i, i)
	}
	for i := 0; i < 10; i++ {
		value, ok := cache.Get(i)
		if !ok {
			t.Errorf("expected value %d to be in cache", i)
		}
		if value != i {
			t.Errorf("expected value %d to be %d", value, i)
		}
	}

	assert.Equal(t, cache.length, 10)
	assert.Equal(t, cache.listHead, cache.listHead.prev.next)

	for i := 0; i < 10; i++ {
		cache.Evict(i, func(i int) bool { return true })
	}
	assert.Equal(t, cache.length, 0)
	assert.Empty(t, cache.listHead)
}
