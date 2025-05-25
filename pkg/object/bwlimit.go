// pkg/object/bwlimit.go

package object

import (
	"fmt"
	"github.com/juju/ratelimit"
	"io"
)

type limitedReader struct {
	io.Reader
	r *ratelimit.Bucket
}

func (l *limitedReader) Read(buf []byte) (int, error) {
	n, err := l.Reader.Read(buf)
	if l.r != nil {
		l.r.Wait(int64(n))
	}
	return n, err
}

// Seek calls the Seek in the underlying reader.
func (l *limitedReader) Seek(offset int64, whence int) (int64, error) {
	if s, ok := l.Reader.(io.Seeker); ok {
		return s.Seek(offset, whence)
	}
	return 0, fmt.Errorf("%+v does not support Seek()", l.Reader)
}

// Close closes the underlying reader
func (l *limitedReader) Close() error {
	if rc, ok := l.Reader.(io.Closer); ok {
		return rc.Close()
	}
	return nil
}

type bwlimit struct {
	ObjectStorage
	upLimit   *ratelimit.Bucket
	downLimit *ratelimit.Bucket
}

func NewLimited(o ObjectStorage, up, down int64) ObjectStorage {
	bw := &bwlimit{o, nil, nil}
	if up > 0 {
		// there are overheads coming from HTTP/TCP/IP
		bw.upLimit = ratelimit.NewBucketWithRate(float64(up)*0.85, up)
	}
	if down > 0 {
		bw.downLimit = ratelimit.NewBucketWithRate(float64(down)*0.85, down)
	}
	return bw
}

func (p *bwlimit) Get(key string, off, limit int64) (io.ReadCloser, error) {
	r, err := p.ObjectStorage.Get(key, off, limit)
	return &limitedReader{r, p.downLimit}, err
}

func (p *bwlimit) Put(key string, in io.Reader) error {
	in = &limitedReader{in, p.upLimit}
	return p.ObjectStorage.Put(key, in)
}
