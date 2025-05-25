// pkg/chunk/page.go

package chunk

import (
    "AveFS/pkg/utils"
    "github.com/pkg/errors"
    "io"
    "runtime"
    "sync/atomic"
)

type Page struct {
    refs    int32
    offHeap bool
    dep     *Page
    Data    []byte
}

// NewPage create a new page.
func NewPage(data []byte) *Page {
    return &Page{refs: 1, Data: data}
}

func NewOffPage(size int) *Page {
    if size <= 0 {
        panic("size of page should > 0")
    }
    p := utils.Alloc(size)
    page := &Page{refs: 1, offHeap: true, Data: p}
    runtime.SetFinalizer(page, func(p *Page) {
        refCnt := atomic.LoadInt32(&p.refs)
        if refCnt != 0 {
            logger.Errorf("refcount of page %p is not zero: %d", p, refCnt)
            if refCnt > 0 {
                p.Release()
            }
        }
    })
    return page
}

func (p *Page) Slice(off, len int) *Page {
    p.Acquire()
    np := NewPage(p.Data[off : off+len])
    np.dep = p
    return np
}

// Acquire increase the refcount
func (p *Page) Acquire() {
    atomic.AddInt32(&p.refs, 1)
}

// Release decreases the refcount
func (p *Page) Release() {
    if atomic.AddInt32(&p.refs, -1) == 0 {
        if p.offHeap {
            utils.Free(p.Data)
        }
        if p.dep != nil {
            p.dep.Release()
            p.dep = nil
        }
        p.Data = nil
    }
}

type PageReader struct {
    p   *Page
    off int
}

func NewPageReader(p *Page) *PageReader {
    p.Acquire()
    return &PageReader{p, 0}
}

func (r *PageReader) Read(buf []byte) (int, error) {
    n, err := r.ReadAt(buf, int64(r.off))
    r.off += n
    return n, err
}

func (r *PageReader) ReadAt(buf []byte, off int64) (int, error) {
    if len(buf) == 0 {
        return 0, nil
    }
    if r.p == nil {
        return 0, errors.New("page is already released")
    }
    if int(off) == len(r.p.Data) {
        return 0, io.EOF
    }
    n := copy(buf, r.p.Data[off:])
    if n < len(buf) {
        return n, io.EOF
    }
    return n, nil
}

func (r *PageReader) Close() error {
    if r.p != nil {
        r.p.Release()
        r.p = nil
    }
    return nil
}
