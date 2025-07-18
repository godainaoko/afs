// pkg/vfs/compact.go

package vfs

import (
	"AveFS/pkg/chunk"
	"AveFS/pkg/meta"
	"AveFS/pkg/utils"
	"context"
	"time"
)

func readSlice(store chunk.ChunkStore, s *meta.Slice, page *chunk.Page, off int) error {
	buf := page.Data
	read := 0
	reader := store.NewReader(s.Chunkid, int(s.Size))
	for read < len(buf) {
		p := page.Slice(read, len(buf)-read)
		n, err := reader.ReadAt(context.Background(), p, off+int(s.Off))
		p.Release()
		if n == 0 && err != nil {
			return err
		}
		read += n
		off += n
	}
	return nil
}

func Compact(conf chunk.Config, store chunk.ChunkStore, slices []meta.Slice, chunkid uint64) error {
	for utils.AllocMemory()-store.UsedMemory() > int64(conf.BufferSize)*3/2 {
		time.Sleep(time.Millisecond * 100)
	}
	var size uint32
	for _, s := range slices {
		size += s.Len
	}
	//compactSizeHistogram.Observe(float64(size))
	logger.Debugf("compact %d slices (%d bytes) to chunk %d", len(slices), size, chunkid)

	writer := store.NewWriter(chunkid)

	var pos int
	for i, s := range slices {
		if s.Chunkid == 0 {
			_, err := writer.WriteAt(make([]byte, int(s.Len)), int64(pos))
			if err != nil {
				writer.Abort()
				return err
			}
			pos += int(s.Len)
			continue
		}
		var read int
		for read < int(s.Len) {
			l := utils.Min(conf.BlockSize, int(s.Len)-read)
			p := chunk.NewOffPage(l)
			if err := readSlice(store, &s, p, read); err != nil {
				logger.Infof("can't compact chunk %d, retry later, read %d: %s", chunkid, i, err)
				p.Release()
				writer.Abort()
				return err
			}
			_, err := writer.WriteAt(p.Data, int64(pos+read))
			p.Release()
			if err != nil {
				logger.Errorf("can't compact chunk %d, retry later, write: %s", chunkid, err)
				writer.Abort()
				return err
			}
			read += l
			if pos+read >= conf.BlockSize {
				if err = writer.FlushTo(pos + read); err != nil {
					panic(err)
				}
			}
		}
		pos += int(s.Len)
	}
	err := writer.Finish(pos)
	if err != nil {
		writer.Abort()
	}
	return err
}
