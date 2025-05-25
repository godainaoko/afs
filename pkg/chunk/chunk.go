// pkg/chunk/chunk.go

package chunk

import (
    "context"
    "io"
)

type Reader interface {
    ReadAt(ctx context.Context, p *Page, off int) (int, error)
}

type Writer interface {
    io.WriterAt
    ID() uint64
    SetID(chunkid uint64)
    FlushTo(offset int) error
    Finish(length int) error
    Abort()
}

type ChunkStore interface {
    NewReader(chunkid uint64, length int) Reader
    NewWriter(chunkid uint64) Writer
    Remove(chunkid uint64, length int) error
    FillCache(chunkid uint64, length uint32) error
    UsedMemory() int64
}
