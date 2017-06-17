package expirecache

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

func expired(e element, u int64) bool {
	if e.evicted {
		return true
	}
	if e.maxAge < u {
		return true
	}
	return false
}

// Element is an element stored in the cache.
// maxAge and arrival are unix timestamps.
type element struct {
	arrival int64
	maxAge  int64
	data    interface{}
	size    uint64
	evicted bool
}

// ExpireCache is an expiring cache.
// Evicted data is returned on the evictions channel.
type ExpireCache struct {
	sync.RWMutex
	cache     map[string]element
	ctx       context.Context
	cancel    context.CancelFunc
	elements  []string
	totalSize uint64
	maxSize   uint64
	evictions chan interface{}
}

// NewExpireCache creates a new expire cache with maximum size, maxSize.
// A channel containing evicted elements is returned along with a pointer to the cache.
func NewExpireCache(ctx context.Context, maxSize uint64) (*ExpireCache, chan interface{}) {
	cacheContext, cancelExpireCache := context.WithCancel(ctx)
	ev := make(chan interface{})
	c := &ExpireCache{
		ctx:       cacheContext,
		cancel:    cancelExpireCache,
		cache:     make(map[string]element),
		maxSize:   maxSize,
		evictions: ev,
	}

	const cleanerDelay = time.Duration(time.Second * 15)
	go func() {
		for {
			c.clean(time.Now())
			select {
			case <-c.ctx.Done():
				c.clean(time.Time{})
				close(c.evictions)
				return
			case <-time.After(cleanerDelay):
				continue
			}
		}
	}()

	return c, c.evictions
}

// Size returns the current memory size of the cache.
func (ec *ExpireCache) Size() uint64 {
	ec.RLock()
	s := ec.totalSize
	ec.RUnlock()
	return s
}

// Get returns the element from the cache.
func (ec *ExpireCache) Get(k string) (element interface{}, ok bool) {
	ec.RLock()
	v, ok := ec.cache[k]
	ec.RUnlock()

	if !ok || expired(v, time.Now().Unix()) {
		// NOTE: deletion now would require a linear search.
		return nil, false
	}

	return v.data, ok
}

// Set adds an element to the cache, with an estimated size and a maximum lifetime specified as a time.Duration.
func (ec *ExpireCache) Set(k string, v interface{}, size uint64, lifetime time.Duration) {
	ec.Lock()
	oldv, ok := ec.cache[k]
	if !ok {
		ec.elements = append(ec.elements, k)
	} else {
		ec.totalSize -= oldv.size
	}
	ec.totalSize += size
	ec.cache[k] = element{maxAge: time.Now().Add(lifetime).Unix(), data: v, size: size, arrival: time.Now().Unix()}

	// evict elements from the cache if the cache has grown too large.
	for ec.maxSize > 0 && ec.totalSize > ec.maxSize {
		ec.randomEvict()
	}
	ec.Unlock()
}

// Evict evicts the element at k by setting the elements evictions field to true
// and pushing the element's data on to the evictions channel.
func (ec *ExpireCache) Evict(k string) (ok bool) {
	ec.RLock()
	v, ok := ec.cache[k]
	ec.RUnlock()
	if ok {
		ec.Lock()
		ec.evictions <- ec.cache[k].data
		v.evicted = true
		// NOTE: we cannot delete here, doing so would require a linear search.
		ec.Unlock()
		return true
	}
	return false
}

// randomEvict evicts a random element.
func (ec *ExpireCache) randomEvict() {
	slot := rand.Intn(len(ec.elements))
	k := ec.elements[slot]
	ec.elements[slot] = ec.elements[len(ec.elements)-1]
	ec.elements = ec.elements[:len(ec.elements)-1]
	v := ec.cache[k]
	ec.totalSize -= v.size
	if !v.evicted {
		ec.evictions <- ec.cache[k].data
	}
	delete(ec.cache, k)
}

// clean runs the approximate cleaner.
func (ec *ExpireCache) clean(now time.Time) {
	const sampleSize = 20
	const rerunCount = 5

	for {
		var cleaned int
		ec.Lock()
		for i := 0; len(ec.elements) > 0 && i < sampleSize; i++ {
			idx := rand.Intn(len(ec.elements))
			k := ec.elements[idx]
			v := ec.cache[k]
			if v.maxAge < time.Now().Unix() {
				ec.totalSize -= v.size
				if !v.evicted {
					ec.evictions <- ec.cache[k].data
				}
				delete(ec.cache, k)
				ec.elements[idx] = ec.elements[len(ec.elements)-1]
				ec.elements = ec.elements[:len(ec.elements)-1]
				cleaned++
			}
		}
		ec.Unlock()
		if cleaned < rerunCount {
			return
		}
	}
}

// Close closes the cache.
func (ec *ExpireCache) Close() {
	ec.cancel()
}
