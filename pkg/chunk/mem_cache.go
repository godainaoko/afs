// pkg/chunk/mem_cache.go

package chunk

import (
    "github.com/pkg/errors"
    "sync"
    "time"
)

type memItem struct {
    atime time.Time
    page  *Page
}

type memCache struct {
    sync.Mutex
    capacity int64
    used     int64
    pages    map[string]memItem
}

func newMemStore(config *Config) *memCache {
    c := &memCache{
        capacity: config.CacheSize << 20,
        pages:    make(map[string]memItem),
    }
    return c
}

func (c *memCache) usedMemory() int64 {
    c.Lock()
    defer c.Unlock()
    return c.used
}

func (c *memCache) stats() (int64, int64) {
    c.Lock()
    defer c.Unlock()
    return int64(len(c.pages)), c.used
}

func (c *memCache) cache(key string, p *Page, force bool) {
    if c.capacity == 0 {
        return
    }
    c.Lock()
    defer c.Unlock()
    if _, ok := c.pages[key]; ok {
        return
    }
    size := int64(cap(p.Data))
    p.Acquire()
    c.pages[key] = memItem{time.Now(), p}
    c.used += size
    if c.used > c.capacity {
        c.cleanup()
    }
}

func (c *memCache) delete(key string, p *Page) {
    size := int64(cap(p.Data))
    c.used -= size
    p.Release()
    delete(c.pages, key)
}

func (c *memCache) remove(key string) {
    c.Lock()
    defer c.Unlock()
    if item, ok := c.pages[key]; ok {
        c.delete(key, item.page)
        logger.Debugf("remove %s from cache", key)
    }
}

func (c *memCache) load(key string) (ReadCloser, error) {
    c.Lock()
    defer c.Unlock()
    if item, ok := c.pages[key]; ok {
        c.pages[key] = memItem{time.Now(), item.page}
        return NewPageReader(item.page), nil
    }
    return nil, errors.New("not found")
}

// locked
func (c *memCache) cleanup() {
    var cnt int
    var lastKey string
    var lastValue memItem
    var now = time.Now()
    // for each two random keys, then compare the access time, evict the older one
    for k, v := range c.pages {
        if cnt == 0 || lastValue.atime.After(v.atime) {
            lastKey = k
            lastValue = v
        }
        cnt++
        if cnt > 1 {
            logger.Debugf("remove %s from cache, age: %d", lastKey, now.Sub(lastValue.atime))
            c.delete(lastKey, lastValue.page)
            cnt = 0
            if c.used < c.capacity {
                break
            }
        }
    }
}

func (c *memCache) stage(key string, data []byte, keepCache bool) (string, error) {
    return "", errors.New("not supported")
}
func (c *memCache) uploaded(key string, size int) {}
func (c *memCache) stagePath(key string) string   { return "" }
