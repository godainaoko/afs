// pkg/fuse/context.go

package fuse

import (
	"AveFS/pkg/meta"
	"AveFS/pkg/vfs"
	"context"
	"github.com/hanwen/go-fuse/v2/fuse"
	"sync"
	"syscall"
	"time"
)

// Ino is an alias to meta.Ino
type Ino = meta.Ino

// Attr is an alias to meta.Attr
type Attr = meta.Attr

// Context is an alias to vfs.LogContext
type Context = vfs.LogContext

type fuseContext struct {
	context.Context
	start    time.Time
	header   *fuse.InHeader
	canceled bool
	cancel   <-chan struct{}
}

var contextPool = sync.Pool{
	New: func() interface{} {
		return &fuseContext{}
	},
}

func newContext(cancel <-chan struct{}, header *fuse.InHeader) *fuseContext {
	ctx := contextPool.Get().(*fuseContext)
	ctx.Context = context.Background()
	ctx.start = time.Now()
	ctx.canceled = false
	ctx.cancel = cancel
	ctx.header = header
	return ctx
}

func releaseContext(ctx *fuseContext) {
	contextPool.Put(ctx)
}

func (c *fuseContext) Uid() uint32 {
	return uint32(c.header.Uid)
}

func (c *fuseContext) Gid() uint32 {
	return uint32(c.header.Gid)
}

func (c *fuseContext) Gids() []uint32 {
	return []uint32{c.header.Gid}
}

func (c *fuseContext) Pid() uint32 {
	return uint32(c.header.Pid)
}

func (c *fuseContext) Duration() time.Duration {
	return time.Since(c.start)
}

func (c *fuseContext) Cancel() {
	c.canceled = true
}

func (c *fuseContext) Canceled() bool {
	if c.canceled {
		return true
	}
	select {
	case <-c.cancel:
		return true
	default:
		return false
	}
}

func (c *fuseContext) WithValue(k, v interface{}) {
	c.Context = context.WithValue(c.Context, k, v)
}

func (c *fuseContext) Err() error {
	return syscall.EINTR
}
