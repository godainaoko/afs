// pkg/chunk/cached_store.go

package chunk

import (
	"AveFS/pkg/compress"
	"AveFS/pkg/object"
	"AveFS/pkg/utils"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const chunkSize = 1 << 26 // 64M
const pageSize = 1 << 16  // 64K
const SlowRequest = time.Second * time.Duration(10)

var logger = utils.GetLogger("avefs")

// chunk for read-only
type rChunk struct {
	id     uint64
	length int
	store  *cachedStore
}

func chunkForRead(id uint64, length int, store *cachedStore) *rChunk {
	return &rChunk{id, length, store}
}

func (c *rChunk) blockSize(idx int) int {
	bsize := c.length - idx*c.store.conf.BlockSize
	if bsize > c.store.conf.BlockSize {
		bsize = c.store.conf.BlockSize
	}
	return bsize
}

func (c *rChunk) key(idx int) string {
	if c.store.conf.Partitions > 1 {
		return fmt.Sprintf("chunks/%02X/%v/%v_%v_%v", c.id%256, c.id/1000/1000, c.id, idx, c.blockSize(idx))
	}
	return fmt.Sprintf("chunks/%v/%v/%v_%v_%v", c.id/1000/1000, c.id/1000, c.id, idx, c.blockSize(idx))
}

func (c *rChunk) index(off int) int {
	return off / c.store.conf.BlockSize
}

func (c *rChunk) keys() []string {
	if c.length <= 0 {
		return nil
	}
	lastIdx := (c.length - 1) / c.store.conf.BlockSize
	keys := make([]string, lastIdx+1)
	for i := 0; i <= lastIdx; i++ {
		keys[i] = c.key(i)
	}
	return keys
}

func (c *rChunk) ReadAt(ctx context.Context, page *Page, off int) (n int, err error) {
	p := page.Data
	if len(p) == 0 {
		return 0, nil
	}
	if off >= c.length {
		return 0, io.EOF
	}

	idx := c.index(off)
	boff := off % c.store.conf.BlockSize
	blockSize := c.blockSize(idx)
	if boff+len(p) > blockSize {
		// read beyond the current page
		var got int
		for got < len(p) {
			// aligned to the current page
			l := utils.Min(len(p)-got, c.blockSize(c.index(off))-off%c.store.conf.BlockSize)
			pp := page.Slice(got, l)
			n, err = c.ReadAt(ctx, pp, off)
			pp.Release()
			if err != nil {
				return got + n, err
			}
			if n == 0 {
				return got, io.EOF
			}
			got += n
			off += n
		}
		return got, nil
	}

	key := c.key(idx)
	if c.store.conf.CacheSize > 0 {
		//start := time.Now()
		r, err := c.store.bcache.load(key)
		if err == nil {
			n, err = r.ReadAt(p, int64(boff))
			r.Close()
			if err == nil {
				//cacheHits.Add(1)
				//cacheHitBytes.Add(float64(n))
				//cacheReadHist.Observe(time.Since(start).Seconds())
				return n, nil
			}
			if f, ok := r.(*os.File); ok {
				logger.Warnf("remove partial cached block %s: %d %s", f.Name(), n, err)
				os.Remove(f.Name())
			}
		}
	}

	//cacheMiss.Add(1)
	//cacheMissBytes.Add(float64(len(p)))

	if c.store.seekable && boff > 0 && len(p) <= blockSize/4 {
		// partial read
		st := time.Now()
		in, err := c.store.storage.Get(key, int64(boff), int64(len(p)))
		used := time.Since(st)
		logger.Debugf("GET %s RANGE(%d,%d) (%s, %.3fs)", key, boff, len(p), err, used.Seconds())
		if used > SlowRequest {
			logger.Infof("slow request: GET %s (%s, %.3fs)", key, err, used.Seconds())
		}
		c.store.fetcher.fetch(key)
		if err == nil {
			defer in.Close()
			return io.ReadFull(in, p)
		}
	}

	block, err := c.store.group.Execute(key, func() (*Page, error) {
		tmp := page
		if boff > 0 || len(p) < blockSize {
			tmp = NewOffPage(blockSize)
		} else {
			tmp.Acquire()
		}
		tmp.Acquire()
		err := withTimeout(func() error {
			defer tmp.Release()
			return c.store.load(key, tmp, c.store.shouldCache(blockSize), false)
		}, c.store.conf.GetTimeout)
		return tmp, err
	})
	defer block.Release()
	if err != nil {
		return 0, err
	}
	if block != page {
		copy(p, block.Data[boff:])
	}
	return len(p), nil
}

func (c *rChunk) delete(idx int) error {
	key := c.key(idx)
	st := time.Now()
	err := c.store.storage.Delete(key)
	used := time.Since(st)
	logger.Debugf("DELETE %v (%v, %.3fs)", key, err, used.Seconds())
	if used > SlowRequest {
		logger.Infof("slow request: DELETE %v (%s, %.3fs)", key, err, used.Seconds())
	}
	return err
}

func (c *rChunk) Remove() error {
	if c.length == 0 {
		// no block
		return nil
	}

	lastIdx := (c.length - 1) / c.store.conf.BlockSize
	var err error
	for i := 0; i <= lastIdx; i++ {
		// there could be multiple clients try to remove the same chunk at the same time;
		// any of them should succeed if any blocks are removed
		key := c.key(i)
		c.store.pendingMutex.Lock()
		delete(c.store.pendingKeys, key)
		c.store.pendingMutex.Unlock()
		c.store.bcache.remove(key)
		if e := c.delete(i); e != nil {
			err = e
		}
	}
	return err
}

var pagePool = make(chan *Page, 128)

func allocPage(sz int) *Page {
	if sz != pageSize {
		return NewOffPage(sz)
	}
	select {
	case p := <-pagePool:
		return p
	default:
		return NewOffPage(pageSize)
	}
}

func freePage(p *Page) {
	if cap(p.Data) != pageSize {
		p.Release()
		return
	}
	select {
	case pagePool <- p:
	default:
		p.Release()
	}
}

// chunk for write-only
type wChunk struct {
	rChunk
	pages       [][]*Page
	uploaded    int
	errors      chan error
	uploadError error
	pendings    int
}

func chunkForWrite(id uint64, store *cachedStore) *wChunk {
	return &wChunk{
		rChunk: rChunk{id, 0, store},
		pages:  make([][]*Page, chunkSize/store.conf.BlockSize),
		errors: make(chan error, chunkSize/store.conf.BlockSize),
	}
}

func (c *wChunk) SetID(id uint64) {
	c.id = id
}

func (c *wChunk) WriteAt(p []byte, off int64) (n int, err error) {
	if int(off)+len(p) > chunkSize {
		return 0, fmt.Errorf("write out of chunk boudary: %d > %d", int(off)+len(p), chunkSize)
	}
	if off < int64(c.uploaded) {
		return 0, fmt.Errorf("cannot overwrite uploaded block: %d < %d", off, c.uploaded)
	}

	// Fill previous blocks with zeros
	if c.length < int(off) {
		zeros := make([]byte, int(off)-c.length)
		_, _ = c.WriteAt(zeros, int64(c.length))
	}

	for n < len(p) {
		idx := c.index(int(off) + n)
		boff := (int(off) + n) % c.store.conf.BlockSize
		var bs = pageSize
		if idx > 0 || bs > c.store.conf.BlockSize {
			bs = c.store.conf.BlockSize
		}
		bi := boff / bs
		bo := boff % bs
		var page *Page
		if bi < len(c.pages[idx]) {
			page = c.pages[idx][bi]
		} else {
			page = allocPage(bs)
			page.Data = page.Data[:0]
			c.pages[idx] = append(c.pages[idx], page)
		}
		left := len(p) - n
		if bo+left > bs {
			page.Data = page.Data[:bs]
		} else if len(page.Data) < bo+left {
			page.Data = page.Data[:bo+left]
		}
		n += copy(page.Data[bo:], p[n:])
	}
	if int(off)+n > c.length {
		c.length = int(off) + n
	}
	return n, nil
}

func withTimeout(f func() error, timeout time.Duration) error {
	var done = make(chan int, 1)
	var t = time.NewTimer(timeout)
	var err error
	go func() {
		err = f()
		done <- 1
	}()
	select {
	case <-done:
		t.Stop()
	case <-t.C:
		err = fmt.Errorf("timeout after %s", timeout)
	}
	return err
}

func (c *wChunk) put(key string, p *Page) error {
	p.Acquire()
	return withTimeout(func() error {
		defer p.Release()
		st := time.Now()
		err := c.store.storage.Put(key, bytes.NewReader(p.Data))
		used := time.Since(st)
		logger.Debugf("PUT %s (%s, %.3fs)", key, err, used.Seconds())
		if used > SlowRequest {
			logger.Infof("slow request: PUT %v (%s, %.3fs)", key, err, used.Seconds())
		}
		return err
	}, c.store.conf.PutTimeout)
}

func (c *wChunk) syncUpload(key string, block *Page) {
	blen := len(block.Data)
	bufSize := c.store.compressor.CompressBound(blen)
	var buf *Page
	if bufSize > blen {
		buf = NewOffPage(bufSize)
	} else {
		buf = block
		buf.Acquire()
	}
	n, err := c.store.compressor.Compress(buf.Data, block.Data)
	if err != nil {
		logger.Fatalf("compress chunk %v: %s", c.id, err)
		return
	}
	buf.Data = buf.Data[:n]
	if blen < c.store.conf.BlockSize {
		// block will be freed after written into the disk
		c.store.bcache.cache(key, block, false)
	}
	block.Release()

	c.store.currentUpload <- true
	defer func() {
		buf.Release()
		<-c.store.currentUpload
	}()

	try := 0
	for try <= 10 && c.uploadError == nil {
		err = c.put(key, buf)
		if err == nil {
			c.errors <- nil
			return
		}
		try++
		logger.Warnf("upload %s: %s (try %d)", key, err, try)
		time.Sleep(time.Second * time.Duration(try*try))
	}
	c.errors <- fmt.Errorf("upload block %s: %s (after %d tries)", key, err, try)
}

func (c *wChunk) asyncUpload(key string, block *Page, stagingPath string) {
	blockSize := len(block.Data)
	defer c.store.bcache.uploaded(key, blockSize)
	defer func() {
		<-c.store.currentUpload
	}()
	select {
	case c.store.currentUpload <- true:
	default:
		// release the memory and wait
		block.Release()
		c.store.pendingMutex.Lock()
		c.store.pendingKeys[key] = time.Now()
		c.store.pendingMutex.Unlock()
		defer func() {
			c.store.pendingMutex.Lock()
			delete(c.store.pendingKeys, key)
			c.store.pendingMutex.Unlock()
		}()

		logger.Debugf("wait to upload %s", key)
		c.store.currentUpload <- true

		// load from disk
		f, err := os.Open(stagingPath)
		if err != nil {
			c.store.pendingMutex.Lock()
			_, ok := c.store.pendingKeys[key]
			c.store.pendingMutex.Unlock()
			if ok {
				logger.Errorf("read stagging file %s: %s", stagingPath, err)
			} else {
				logger.Debugf("%s is not needed, drop it", key)
			}
			return
		}

		block = NewOffPage(blockSize)
		_, err = io.ReadFull(f, block.Data)
		f.Close()
		if err != nil {
			logger.Errorf("read stagging file %s: %s", stagingPath, err)
			block.Release()
			return
		}
	}
	bufSize := c.store.compressor.CompressBound(blockSize)
	var buf *Page
	if bufSize > blockSize {
		buf = NewOffPage(bufSize)
	} else {
		buf = block
		buf.Acquire()
	}
	n, err := c.store.compressor.Compress(buf.Data, block.Data)
	if err != nil {
		logger.Fatalf("compress chunk %v: %s", c.id, err)
		return
	}
	buf.Data = buf.Data[:n]
	block.Release()

	try := 0
	for c.uploadError == nil {
		err = c.put(key, buf)
		if err == nil {
			break
		}
		logger.Warnf("upload %s: %s (tried %d)", key, err, try)
		try++
		time.Sleep(time.Second * time.Duration(try))
	}
	buf.Release()
	os.Remove(stagingPath)
}

func (c *wChunk) upload(idx int) {
	blen := c.blockSize(idx)
	key := c.key(idx)
	pages := c.pages[idx]
	c.pages[idx] = nil
	c.pendings++

	go func() {
		var block *Page
		if len(pages) == 1 {
			block = pages[0]
		} else {
			block = NewOffPage(blen)
			var off int
			for _, b := range pages {
				off += copy(block.Data[off:], b.Data)
				freePage(b)
			}
			if off != blen {
				logger.Fatalf("block length does not match: %v != %v", off, blen)
			}
		}
		if c.store.conf.Writeback {
			stagingPath, err := c.store.bcache.stage(key, block.Data, c.store.shouldCache(blen))
			if err != nil {
				logger.Warnf("write %s to disk: %s, upload it directly", stagingPath, err)
				c.syncUpload(key, block)
			} else {
				c.errors <- nil
				if c.store.conf.UploadDelay == 0 {
					go c.asyncUpload(key, block, stagingPath)
				} else {
					block.Release()
					c.store.pendingMutex.Lock()
					c.store.pendingKeys[key] = time.Now()
					c.store.pendingMutex.Unlock()
				}
			}
		} else {
			c.syncUpload(key, block)
		}
	}()
}

func (c *wChunk) ID() uint64 {
	return c.id
}

func (c *wChunk) Len() int {
	return c.length
}

func (c *wChunk) FlushTo(offset int) error {
	if offset < c.uploaded {
		logger.Fatalf("Invalid offset: %d < %d", offset, c.uploaded)
	}
	for i, block := range c.pages {
		start := i * c.store.conf.BlockSize
		end := start + c.store.conf.BlockSize
		if start >= c.uploaded && end <= offset {
			if block != nil {
				c.upload(i)
			}
			c.uploaded = end
		}
	}

	return nil
}

func (c *wChunk) Finish(length int) error {
	if c.length != length {
		return fmt.Errorf("length mismatch: %v != %v", c.length, length)
	}

	n := (length-1)/c.store.conf.BlockSize + 1
	if err := c.FlushTo(n * c.store.conf.BlockSize); err != nil {
		return err
	}
	for i := 0; i < c.pendings; i++ {
		if err := <-c.errors; err != nil {
			c.uploadError = err
			return err
		}
	}
	return nil
}

func (c *wChunk) Abort() {
	for i := range c.pages {
		for _, b := range c.pages[i] {
			freePage(b)
		}
		c.pages[i] = nil
	}
	// delete uploaded blocks
	c.length = c.uploaded
	_ = c.Remove()
}

// Config contains options for cachedStore
type Config struct {
	CacheDir       string
	CacheMode      os.FileMode
	CacheSize      int64
	FreeSpace      float32
	AutoCreate     bool
	Compress       string
	MaxUpload      int
	Writeback      bool
	UploadDelay    time.Duration
	Partitions     int
	BlockSize      int
	GetTimeout     time.Duration
	PutTimeout     time.Duration
	CacheFullBlock bool
	BufferSize     int
	Readahead      int
	Prefetch       int
}

type cachedStore struct {
	storage       object.ObjectStorage
	bcache        CacheManager
	fetcher       *prefetcher
	conf          Config
	group         *Controller
	currentUpload chan bool
	pendingKeys   map[string]time.Time
	pendingMutex  sync.Mutex
	compressor    compress.Compressor
	seekable      bool
}

func (store *cachedStore) load(key string, page *Page, cache bool, forceCache bool) (err error) {
	defer func() {
		e := recover()
		if e != nil {
			err = fmt.Errorf("recovered from %s", e)
		}
	}()

	err = errors.New("not downloaded")
	var in io.ReadCloser
	tried := 0
	start := time.Now()
	// it will be retried outside
	for err != nil && tried < 2 {
		time.Sleep(time.Second * time.Duration(tried*tried))
		st := time.Now()
		in, err = store.storage.Get(key, 0, -1)
		used := time.Since(st)
		logger.Debugf("GET %s (%s, %.3fs)", key, err, used.Seconds())
		if used > SlowRequest {
			logger.Infof("slow request: GET %s (%s, %.3fs)", key, err, used.Seconds())
		}
		tried++
	}
	if err != nil {
		return fmt.Errorf("get %s: %s", key, err)
	}
	defer in.Close()
	needed := store.compressor.CompressBound(len(page.Data))
	var n int
	if needed > len(page.Data) {
		c := NewOffPage(needed)
		defer c.Release()
		var cn int
		cn, err = io.ReadFull(in, c.Data)
		if err != nil && (cn == 0 || !errors.Is(err, io.ErrUnexpectedEOF)) {
			return err
		}
		n, err = store.compressor.Decompress(page.Data, c.Data[:cn])
	} else {
		n, err = io.ReadFull(in, page.Data)
	}
	if err != nil || n < len(page.Data) {
		return fmt.Errorf("read %s fully: %s (%d < %d) after %s (tried %d)", key, err, n, len(page.Data),
			time.Since(start), tried)
	}
	if cache {
		store.bcache.cache(key, page, forceCache)
	}
	return nil
}

// NewCachedStore create a cached store.
func NewCachedStore(storage object.ObjectStorage, config Config) ChunkStore {
	compressor := compress.NewCompressor(config.Compress)
	if compressor == nil {
		logger.Fatalf("unknown compress algorithm: %s", config.Compress)
	}
	if config.GetTimeout == 0 {
		config.GetTimeout = time.Second * 60
	}
	if config.PutTimeout == 0 {
		config.PutTimeout = time.Second * 60
	}
	store := &cachedStore{
		storage:       storage,
		conf:          config,
		currentUpload: make(chan bool, config.MaxUpload),
		compressor:    compressor,
		seekable:      compressor.CompressBound(0) == 0,
		pendingKeys:   make(map[string]time.Time),
		group:         &Controller{},
	}
	store.bcache = newCacheManager(&config, store.uploadStagingFile)
	if config.CacheSize == 0 {
		config.Prefetch = 0 // disable prefetch if the cache is disabled
	}
	store.fetcher = newPrefetcher(config.Prefetch, func(key string) {
		size := parseObjOrigSize(key)
		if size == 0 || size > store.conf.BlockSize {
			return
		}
		p := NewOffPage(size)
		defer p.Release()
		_ = store.load(key, p, true, true)
	})

	if store.conf.CacheDir != "memory" && store.conf.Writeback && store.conf.UploadDelay > 0 {
		logger.Infof("delay uploading by %s", store.conf.UploadDelay)
		go func() {
			for {
				if store.conf.UploadDelay > time.Minute {
					time.Sleep(time.Minute)
				} else {
					time.Sleep(store.conf.UploadDelay)
				}
				store.uploadDelayedStaging()
			}
		}()
	}
	return store
}

func (store *cachedStore) shouldCache(size int) bool {
	return size < store.conf.BlockSize || store.conf.CacheFullBlock
}

func parseObjOrigSize(key string) int {
	p := strings.LastIndexByte(key, '_')
	l, _ := strconv.Atoi(key[p+1:])
	return l
}

func (store *cachedStore) uploadStagingFile(key string, stagingPath string) {
	store.currentUpload <- true
	go func() {
		defer func() {
			<-store.currentUpload
		}()

		f, err := os.Open(stagingPath)
		if err != nil {
			logger.Errorf("open %s: %s", stagingPath, err)
			return
		}
		blockSize := parseObjOrigSize(key)
		block := NewOffPage(blockSize)
		_, err = io.ReadFull(f, block.Data)
		f.Close()
		if err != nil {
			block.Release()
			logger.Errorf("read %s: %s", stagingPath, err)
			return
		}
		buf := NewOffPage(store.compressor.CompressBound(blockSize))
		defer buf.Release()
		n, err := store.compressor.Compress(buf.Data, block.Data)
		block.Release()
		if err != nil {
			logger.Errorf("compress chunk %s: %s", stagingPath, err)
			return
		}
		compressed := buf.Data[:n]
		try := 0
		for {
			st := time.Now()
			err := store.storage.Put(key, bytes.NewReader(compressed))
			used := time.Since(st)
			logger.Debugf("PUT %s (%s, %.3fs)", key, err, used.Seconds())
			if used > SlowRequest {
				logger.Infof("slow request: PUT %v (%s, %.3fs)", key, err, used.Seconds())
			}
			if err == nil {
				break
			}
			logger.Warnf("upload %s: %s (try %d)", key, err, try)
			try++
			time.Sleep(time.Second * time.Duration(try*try))
		}
		store.bcache.uploaded(key, blockSize)
		store.pendingMutex.Lock()
		delete(store.pendingKeys, key)
		store.pendingMutex.Unlock()
		_ = os.Remove(stagingPath)
	}()
}

func (store *cachedStore) uploadDelayedStaging() {
	store.pendingMutex.Lock()
	cutoff := time.Now().Add(-store.conf.UploadDelay)
	for key, added := range store.pendingKeys {
		store.pendingMutex.Unlock()
		if added.Before(cutoff) {
			store.uploadStagingFile(key, store.bcache.stagePath(key))
		}
		store.pendingMutex.Lock()
	}
	store.pendingMutex.Unlock()
}

func (store *cachedStore) NewReader(chunkid uint64, length int) Reader {
	return chunkForRead(chunkid, length, store)
}

func (store *cachedStore) NewWriter(chunkid uint64) Writer {
	return chunkForWrite(chunkid, store)
}

func (store *cachedStore) Remove(chunkid uint64, length int) error {
	r := chunkForRead(chunkid, length, store)
	return r.Remove()
}

func (store *cachedStore) FillCache(chunkid uint64, length uint32) error {
	r := chunkForRead(chunkid, int(length), store)
	keys := r.keys()
	for _, k := range keys {
		f, err := store.bcache.load(k)
		if err == nil { // already cached
			f.Close()
			continue
		}
		size := parseObjOrigSize(k)
		if size == 0 || size > store.conf.BlockSize {
			logger.Warnf("Invalid size: %s %d", k, size)
			continue
		}
		p := NewOffPage(size)
		defer p.Release() // NOTE: defer called in a for-loop
		if err = store.load(k, p, true, true); err != nil {
			logger.Warnf("Failed to load key: %s %s", k, err)
		}
	}
	return nil // currently errors are skipped
}

func (store *cachedStore) UsedMemory() int64 {
	return store.bcache.usedMemory()
}

var _ ChunkStore = &cachedStore{}
