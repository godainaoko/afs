// pkg/chunk/singleflight.go

package chunk

import "sync"

type request struct {
    wg  sync.WaitGroup
    val *Page
    ref int
    err error
}

type Controller struct {
    sync.Mutex
    rs map[string]*request
}

func (con *Controller) Execute(key string, fn func() (*Page, error)) (*Page, error) {
    con.Lock()
    if con.rs == nil {
        con.rs = make(map[string]*request)
    }
    if c, ok := con.rs[key]; ok {
        c.ref++
        con.Unlock()
        c.wg.Wait()
        c.val.Acquire()
        con.Lock()
        c.ref--
        if c.ref == 0 {
            c.val.Release()
        }
        con.Unlock()
        return c.val, c.err
    }
    c := new(request)
    c.wg.Add(1)
    c.ref++
    con.rs[key] = c
    con.Unlock()

    c.val, c.err = fn()
    c.val.Acquire()
    c.wg.Done()

    con.Lock()
    c.ref--
    if c.ref == 0 {
        c.val.Release()
    }
    delete(con.rs, key)
    con.Unlock()

    return c.val, c.err
}
