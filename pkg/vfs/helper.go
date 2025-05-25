// pkg/vfs/helper.go

package vfs

import (
    "AveFS/pkg/meta"
    "fmt"
    "syscall"
    "time"
)

const (
    MODE_MASK_X = 1 << iota
    MODE_MASK_W
    MODE_MASK_R
)

func strerr(errno syscall.Errno) string {
    if errno == 0 {
        return "OK"
    }
    return errno.Error()
}

var typeStr = map[uint16]byte{
    syscall.S_IFSOCK: 's',
    syscall.S_IFLNK:  'l',
    syscall.S_IFREG:  '-',
    syscall.S_IFBLK:  'b',
    syscall.S_IFDIR:  'd',
    syscall.S_IFCHR:  'c',
    syscall.S_IFIFO:  'f',
    0:                '?',
}

type smode uint16

func (mode smode) String() string {
    s := []byte("?rwxrwxrwx")
    s[0] = typeStr[uint16(mode)&(syscall.S_IFMT&0xffff)]
    if (mode & syscall.S_ISUID) != 0 {
        s[3] = 's'
    }
    if (mode & syscall.S_ISGID) != 0 {
        s[6] = 's'
    }
    if (mode & syscall.S_ISVTX) != 0 {
        s[9] = 't'
    }
    for i := uint16(0); i < 9; i++ {
        if (mode & (1 << i)) == 0 {
            if s[9-i] == 's' || s[9-i] == 't' {
                s[9-i] &= 0xDF
            } else {
                s[9-i] = '-'
            }
        }
    }
    return string(s)
}

// Entry is an alias of meta.Entry, which is used to generate the string
// representation lazily.
type Entry meta.Entry

func (entry *Entry) String() string {
    if entry == nil {
        return ""
    }
    if entry.Attr == nil {
        return fmt.Sprintf(" (%d)", entry.Inode)
    }
    a := entry.Attr
    mode := a.SMode()
    return fmt.Sprintf(" (%d,[%s:0%06o,%d,%d,%d,%d,%d,%d,%d])",
        entry.Inode, smode(mode), mode, a.Nlink, a.Uid, a.Gid,
        a.Atime, a.Mtime, a.Ctime, a.Length)
}

// LogContext is an interface to add duration on meta.Context.
type LogContext interface {
    meta.Context
    Duration() time.Duration
}

type logContext struct {
    meta.Context
    start time.Time
}

func (ctx *logContext) Duration() time.Duration {
    return time.Since(ctx.start)
}

// NewLogContext creates an LogContext starting from now.
func NewLogContext(ctx meta.Context) LogContext {
    return &logContext{ctx, time.Now()}
}
