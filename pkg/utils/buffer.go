// pkg/utils/buffer.go

package utils

import (
    "encoding/binary"
    "unsafe"
)

type Buffer struct {
    endian binary.ByteOrder
    off    int
    buf    []byte
}

// FromBuffer utility to create *Buffer
func FromBuffer(buf []byte) *Buffer {
    return &Buffer{binary.BigEndian, 0, buf}
}

// NewBuffer returns a buffer with sz number of bytes.
func NewBuffer(sz uint32) *Buffer {
    return FromBuffer(make([]byte, sz))
}

// ReadBuffer utility to create *Buffer from a slice of bytes
func ReadBuffer(buf []byte) *Buffer {
    return FromBuffer(buf)
}

// Len returns the length of buffer
func (b *Buffer) Len() int {
    return len(b.buf)
}

// HasMore checks if the offset is less than length
func (b *Buffer) HasMore() bool {
    return b.off < len(b.buf)
}

// Left returns the number of bytes after offset
func (b *Buffer) Left() int {
    return len(b.buf) - b.off
}

// Seek seeks or sets offset to `p`
func (b *Buffer) Seek(p int) {
    b.off = p
}

// Buffer returns
func (b *Buffer) Buffer() []byte {
    return b.buf[b.off:]
}

// Put8 appends uint8 to Buffer
func (b *Buffer) Put8(v uint8) {
    b.buf[b.off] = v
    b.off++
}

// Get8 returns uint8
func (b *Buffer) Get8() uint8 {
    v := b.buf[b.off]
    b.off++
    return v
}

// Put16 appends uint16 to Buffer
func (b *Buffer) Put16(v uint16) {
    b.endian.PutUint16(b.buf[b.off:b.off+2], v)
    b.off += 2
}

// Get16 returns uint16
func (b *Buffer) Get16() uint16 {
    v := b.endian.Uint16(b.buf[b.off : b.off+2])
    b.off += 2
    return v
}

// Put32 appends uint32 to Buffer
func (b *Buffer) Put32(v uint32) {
    b.endian.PutUint32(b.buf[b.off:b.off+4], v)
    b.off += 4
}

// Get32 returns uint32
func (b *Buffer) Get32() uint32 {
    v := b.endian.Uint32(b.buf[b.off : b.off+4])
    b.off += 4
    return v
}

// Put64 appends uint64 to Buffer
func (b *Buffer) Put64(v uint64) {
    b.endian.PutUint64(b.buf[b.off:b.off+8], v)
    b.off += 8
}

// Get64 returns uint64
func (b *Buffer) Get64() uint64 {
    v := b.endian.Uint64(b.buf[b.off : b.off+8])
    b.off += 8
    return v
}

// Put appends a slice of byte to Buffer
func (b *Buffer) Put(v []byte) {
    l := len(v)
    copy(b.buf[b.off:b.off+l], v)
    b.off += l
}

// Get returns `l` bytes from offset
func (b *Buffer) Get(l int) []byte {
    b.off += l
    return b.buf[b.off-l : b.off]
}

// SetBytes initializes the Buffer with BigEndian ordering
func (b *Buffer) SetBytes(buf []byte) {
    b.endian = binary.BigEndian
    b.off = 0
    b.buf = buf
}

// Bytes return the bytes
func (b *Buffer) Bytes() []byte {
    return b.buf
}

var nativeEndian binary.ByteOrder

// NewNativeBuffer utility to create *Buffer of a given size with nativeEndian
func NewNativeBuffer(buf []byte) *Buffer {
    return &Buffer{nativeEndian, 0, buf}
}

func init() {
    buf := [2]byte{}
    *(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

    switch buf {
    case [2]byte{0xCD, 0xAB}:
        nativeEndian = binary.LittleEndian
    case [2]byte{0xAB, 0xCD}:
        nativeEndian = binary.BigEndian
    default:
        panic("Could not determine native endianness.")
    }
}
