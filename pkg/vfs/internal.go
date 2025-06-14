// pkg/vfs/internal.go

package vfs

import (
	"AveFS/pkg/meta"
	"AveFS/pkg/utils"
	"bytes"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"
)

const (
	minInternalNode = 0x7FFFFFFFFFFFF0
	logInode        = minInternalNode + 1
	controlInode    = minInternalNode + 2
	statsInode      = minInternalNode + 3
	configInode     = minInternalNode + 4
)

type internalNode struct {
	inode Ino
	name  string
	attr  *Attr
}

var internalNodes = []*internalNode{
	{logInode, ".accesslog", &Attr{Mode: 0400}},
	{controlInode, ".control", &Attr{Mode: 0666}},
	{statsInode, ".stats", &Attr{Mode: 0444}},
	{configInode, ".config", &Attr{Mode: 0400}},
}

func init() {
	uid := uint32(os.Getuid())
	gid := uint32(os.Getgid())
	now := time.Now().Unix()
	for _, v := range internalNodes {
		v.attr.Typ = meta.TypeFile
		v.attr.Uid = uid
		v.attr.Gid = gid
		v.attr.Atime = now
		v.attr.Mtime = now
		v.attr.Ctime = now
		v.attr.Nlink = 1
		v.attr.Full = true
	}
}

func IsSpecialNode(ino Ino) bool {
	return ino >= minInternalNode
}

func IsSpecialName(name string) bool {
	if name[0] != '.' {
		return false
	}
	for _, n := range internalNodes {
		if name == n.name {
			return true
		}
	}
	return false
}

func getInternalNode(ino Ino) *internalNode {
	for _, n := range internalNodes {
		if ino == n.inode {
			return n
		}
	}
	return nil
}

func GetInternalNodeByName(name string) (Ino, *Attr) {
	n := getInternalNodeByName(name)
	if n != nil {
		return n.inode, n.attr
	}
	return 0, nil
}

func getInternalNodeByName(name string) *internalNode {
	if name[0] != '.' {
		return nil
	}
	for _, n := range internalNodes {
		if name == n.name {
			return n
		}
	}
	return nil
}

func handleInternalMsg(ctx Context, msg []byte) []byte {
	r := utils.ReadBuffer(msg)
	cmd := r.Get32()
	size := int(r.Get32())
	if r.Left() != size {
		logger.Warnf("broken message: %d %d != %d", cmd, size, r.Left())
		return []byte{uint8(syscall.EIO & 0xff)}
	}
	switch cmd {
	case meta.Rmr:
		inode := Ino(r.Get64())
		name := string(r.Get(int(r.Get8())))
		r := meta.Remove(m, ctx, inode, name)
		return []byte{uint8(r)}
	case meta.Info:
		var summary meta.Summary
		inode := Ino(r.Get64())

		wb := utils.NewBuffer(4)
		r := meta.GetSummary(m, ctx, inode, &summary)
		if r != 0 {
			msg := r.Error()
			wb.Put32(uint32(len(msg)))
			return append(wb.Bytes(), []byte(msg)...)
		}
		var w = bytes.NewBuffer(nil)
		fmt.Fprintf(w, " inode: %d\n", inode)
		fmt.Fprintf(w, " files:\t%d\n", summary.Files)
		fmt.Fprintf(w, " dirs:\t%d\n", summary.Dirs)
		fmt.Fprintf(w, " length:\t%d\n", summary.Length)
		fmt.Fprintf(w, " size:\t%d\n", summary.Size)

		if summary.Files == 1 && summary.Dirs == 0 {
			fmt.Fprintf(w, " chunks:\n")
			for idx := uint64(0); idx*meta.ChunkSize < summary.Length; idx++ {
				var cs []meta.Slice
				_ = m.Read(ctx, inode, uint32(idx), &cs)
				for _, c := range cs {
					fmt.Fprintf(w, "\t%d:\t%d\t%d\t%d\t%d\n", idx, c.Chunkid, c.Size, c.Off, c.Len)
				}
			}
		}
		wb.Put32(uint32(w.Len()))
		return append(wb.Bytes(), w.Bytes()...)
	case meta.FillCache:
		paths := strings.Split(string(r.Get(int(r.Get32()))), "\n")
		concurrent := r.Get16()
		background := r.Get8()
		if background == 0 {
			fillCache(paths, int(concurrent))
		} else {
			go fillCache(paths, int(concurrent))
		}
		return []byte{uint8(0)}
	default:
		logger.Warnf("unknown message type: %d", cmd)
		return []byte{uint8(syscall.EINVAL & 0xff)}
	}
}
