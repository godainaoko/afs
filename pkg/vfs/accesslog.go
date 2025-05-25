// pkg/vfs/accesslog.go

package vfs

import (
	"AveFS/pkg/utils"
	"fmt"
	"sync"
	"time"
)

type logReader struct {
	sync.Mutex
	buffer chan []byte
	last   []byte
}

var (
	readerLock sync.Mutex
	readers    map[uint64]*logReader
)

func init() {
	readers = make(map[uint64]*logReader)
}

func logit(ctx Context, format string, args ...interface{}) {
	used := ctx.Duration()
	//opsDurationsHistogram.Observe(used.Seconds())
	readerLock.Lock()
	defer readerLock.Unlock()
	if len(readers) == 0 && used < time.Second*10 {
		return
	}

	cmd := fmt.Sprintf(format, args...)
	t := utils.Now()
	ts := t.Format("2006.01.02 15:04:05.000000")
	cmd += fmt.Sprintf(" <%.6f>", used.Seconds())
	if ctx.Pid() != 0 && used >= time.Second*10 {
		logger.Infof("slow operation: %s", cmd)
	}
	line := []byte(fmt.Sprintf("%s [uid:%d,gid:%d,pid:%d] %s\n", ts, ctx.Uid(), ctx.Gid(), ctx.Pid(), cmd))

	for _, r := range readers {
		select {
		case r.buffer <- line:
		default:
		}
	}
}

func openAccessLog(fh uint64) uint64 {
	readerLock.Lock()
	defer readerLock.Unlock()
	readers[fh] = &logReader{buffer: make(chan []byte, 10240)}
	return fh
}

func closeAccessLog(fh uint64) {
	readerLock.Lock()
	defer readerLock.Unlock()
	delete(readers, fh)
}

func readAccessLog(fh uint64, buf []byte) int {
	readerLock.Lock()
	r, ok := readers[fh]
	readerLock.Unlock()
	if !ok {
		return 0
	}
	r.Lock()
	defer r.Unlock()
	var n int
	if len(r.last) > 0 {
		n = copy(buf, r.last)
		r.last = r.last[n:]
	}
	var t = time.NewTimer(time.Second)
	defer t.Stop()
	for n < len(buf) {
		select {
		case line := <-r.buffer:
			l := copy(buf[n:], line)
			n += l
			if l < len(line) {
				r.last = line[l:]
				return n
			}
		case <-t.C:
			if n == 0 {
				n = copy(buf, []byte("#\n"))
			}
			return n
		}
	}
	return n
}
